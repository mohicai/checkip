import asyncio
import aiohttp
import os
import ipaddress  # 引入 IP 地址处理库
from datetime import datetime

# --- 配置区 ---
URL_LIST_FILE = 'url.txt'
OUTPUT_FILE = 'valid_ips.txt'  # 保存有效 IP 的文件名
CHECK_API = 'https://api.090227.xyz/check'
CONCURRENT_LIMIT = 50

# 脚本会自动从 GitHub Actions 的运行环境中读取这些加密后的值
CF_API_TOKEN = os.getenv('CF_API_TOKEN')
CF_ZONE_ID = os.getenv('CF_ZONE_ID')

DNS_RECORD_NAME = 'dns.mtd.dpdns.org'
# --- --- --- ---

def log(message, level="INFO"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")

async def fetch_ips(session, url):
    log(f"正在读取源: {url}")
    try:
        async with session.get(url, timeout=15) as resp:
            if resp.status == 200:
                text = await resp.text()
                ips = [line.strip() for line in text.splitlines() if line.strip()]
                log(f"成功从 {url} 获取到 {len(ips)} 行数据(含潜在网段)", "SUCCESS")
                return ips
            log(f"读取失败 {url}, 状态码: {resp.status}", "WARNING")
    except Exception as e:
        log(f"访问异常 {url}: {str(e)}", "ERROR")
    return []

def parse_and_expand_ips(raw_lines):
    """
    解析原始数据：如果是单个 IP 直接保留；如果是 CIDR 网段则解开为单个 IP。
    同时自动过滤非法的 IP 格式。
    """
    expanded_ips = set()
    for line in raw_lines:
        line = line.strip()
        if not line or line.startswith('#'): # 略过空行或注释
            continue
        try:
            # 尝试将其作为网段或单个IP解析
            network = ipaddress.ip_network(line, strict=False)
            # num_addresses 可以获取该网段包含的 IP 数量
            # 避免意外填入巨大的网段（如 /8）导致内存溢出，这里做个安全限制（例如最大释放 65536 个 IP，即 /16）
            if network.num_addresses > 65536:
                log(f"网段过多或范围过大，已跳过: {line} (包含 {network.num_addresses} 个 IP)", "WARNING")
                continue
                
            for ip in network:
                expanded_ips.add(str(ip))
        except ValueError:
            # 格式不符合 IP 或网段，可能是域名或乱码，直接忽略
            pass
    return list(expanded_ips)

async def check_proxy(session, ip, semaphore):
    async with semaphore:
        try:
            async with session.get(CHECK_API, params={'proxyip': ip}, timeout=12) as resp:
                data = await resp.json()
                if data.get('success') is True:
                    log(f"检测通过: {ip}", "PASS")
                    return ip
        except:
            pass
    return None

async def update_cf_dns(valid_ips):
    if not CF_API_TOKEN or not CF_ZONE_ID:
        log("缺失 CF 环境变量，跳过 DNS 更新步骤", "WARNING")
        return

    headers = {
        "Authorization": f"Bearer {CF_API_TOKEN}",
        "Content-Type": "application/json"
    }
    base_url = f"https://api.cloudflare.com/client/v4/zones/{CF_ZONE_ID}/dns_records"

    async with aiohttp.ClientSession(headers=headers) as session:
        log(f"开始查询 Cloudflare 记录: {DNS_RECORD_NAME} ...")
        async with session.get(base_url, params={"name": DNS_RECORD_NAME, "type": "A"}) as resp:
            if resp.status != 200:
                log(f"无法获取 DNS 记录，请检查 Token 权限和 Zone ID", "ERROR")
                return
            records = (await resp.json()).get('result', [])

        if records:
            log(f"发现 {len(records)} 条属于 {DNS_RECORD_NAME} 的旧记录，准备清理...")
            for r in records:
                if r['name'] == DNS_RECORD_NAME:
                    async with session.delete(f"{base_url}/{r['id']}") as del_resp:
                        if del_resp.status == 200:
                            log(f"已删除旧记录: {r['content']} (ID: {r['id']})", "DEBUG")
                        else:
                            log(f"删除失败: {r['content']}", "WARNING")
            log("所有旧记录清理完毕", "SUCCESS")
        else:
            log("未发现旧记录，无需清理。", "INFO")
        
        if not valid_ips:
            log("没有可用的有效 IP，本次不写入 DNS。", "WARNING")
            return

        log(f"准备写入 {len(valid_ips)} 条新记录到 {DNS_RECORD_NAME}...")
        for ip in valid_ips:
            payload = {
                "type": "A",
                "name": DNS_RECORD_NAME,
                "content": ip,
                "ttl": 60,
                "proxied": False
            }
            async with session.post(base_url, json=payload) as add_resp:
                if add_resp.status == 200:
                    log(f"成功写入 CF 记录: {ip}", "SUCCESS")
                else:
                    err_msg = await add_resp.text()
                    log(f"写入 CF 失败 {ip}: {err_msg}", "ERROR")

async def main():
    start_time = datetime.now()
    log("=== 任务开始 ===")

    async with aiohttp.ClientSession() as session:
        if not os.path.exists(URL_LIST_FILE):
            log(f"找不到文件: {URL_LIST_FILE}", "ERROR")
            return

        with open(URL_LIST_FILE, 'r') as f:
            urls = [line.strip() for line in f if line.strip()]
        
        log(f"正在处理 {len(urls)} 个订阅源 URL...")
        fetch_results = await asyncio.gather(*(fetch_ips(session, u) for u in urls))
        
        # 整合所有行数据
        all_raw_lines = [line for sub in fetch_results for line in sub]
        
        # 【核心修改点】调用解析函数，将包含的 CIDR 网段彻底展开为独立 IP 并去重
        all_ips = parse_and_expand_ips(all_raw_lines)
        
        log(f"网段解开并去重后，共计 {len(all_ips)} 个候选 IP，启动 {CONCURRENT_LIMIT} 并发检测...")
        
        if not all_ips:
            log("没有解析到任何有效的候选 IP，任务结束。", "WARNING")
            return

        semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)
        check_tasks = [check_proxy(session, ip, semaphore) for ip in all_ips]
        check_results = await asyncio.gather(*check_tasks)
        
        valid_ips = [r for r in check_results if r]
        log(f"检测结束。发现可用 IP: {len(valid_ips)} 个")

        try:
            with open(OUTPUT_FILE, 'w') as f:
                f.write('\n'.join(valid_ips))
            log(f"结果已同步保存至本地文件: {OUTPUT_FILE}", "SUCCESS")
        except Exception as e:
            log(f"保存文件失败: {str(e)}", "ERROR")

        await update_cf_dns(valid_ips)

    duration = datetime.now() - start_time
    log(f"=== 任务完成，总耗时: {duration.total_seconds():.1f}秒 ===")

if __name__ == '__main__':
    asyncio.run(main())