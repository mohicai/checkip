import asyncio
import aiohttp
import os
from datetime import datetime

# --- 配置区 ---
URL_LIST_FILE = 'url.txt'
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
                log(f"成功从 {url} 获取到 {len(ips)} 个 IP", "SUCCESS")
                return ips
            log(f"读取失败 {url}, 状态码: {resp.status}", "WARNING")
    except Exception as e:
        log(f"访问异常 {url}: {str(e)}", "ERROR")
    return []

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
    headers = {
        "Authorization": f"Bearer {CF_API_TOKEN}",
        "Content-Type": "application/json"
    }
    base_url = f"https://api.cloudflare.com/client/v4/zones/{CF_ZONE_ID}/dns_records"

    async with aiohttp.ClientSession(headers=headers) as session:
        # 1. 获取现有记录
        log(f"开始查询 Cloudflare 记录: {DNS_RECORD_NAME} ...")
        async with session.get(base_url, params={"name": DNS_RECORD_NAME, "type": "A"}) as resp:
            if resp.status != 200:
                log(f"无法获取 DNS 记录，请检查 Token 权限和 Zone ID", "ERROR")
                return
            records = (await resp.json()).get('result', [])

        # 2. 安全删除旧记录
        if records:
            log(f"发现 {len(records)} 条属于 {DNS_RECORD_NAME} 的旧记录，准备清理...")
            for r in records:
                # 再次核对域名，双重保险
                if r['name'] == DNS_RECORD_NAME:
                    async with session.delete(f"{base_url}/{r['id']}") as del_resp:
                        if del_resp.status == 200:
                            log(f"已删除旧记录: {r['content']} (ID: {r['id']})", "DEBUG")
                        else:
                            log(f"删除失败: {r['content']}", "WARNING")
            log("所有旧记录清理完毕", "SUCCESS")
        else:
            log("未发现旧记录，无需清理。", "INFO")
        
        # 3. 批量添加新记录
        if not valid_ips:
            log("没有可用的有效 IP，本次不更新 DNS。", "WARNING")
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
                    log(f"成功写入记录: {ip}", "SUCCESS")
                else:
                    err_msg = await add_resp.text()
                    log(f"写入失败 {ip}: {err_msg}", "ERROR")

async def main():
    if not CF_API_TOKEN or not CF_ZONE_ID:
        log("缺失环境变量 CF_API_TOKEN 或 CF_ZONE_ID", "ERROR")
        return

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
        all_ips = list(set([ip for sub in fetch_results for ip in sub]))
        
        log(f"去重后共计 {len(all_ips)} 个候选 IP，启动 {CONCURRENT_LIMIT} 并发检测...")
        
        semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)
        check_tasks = [check_proxy(session, ip, semaphore) for ip in all_ips]
        check_results = await asyncio.gather(*check_tasks)
        
        valid_ips = [r for r in check_results if r]
        log(f"检测结束。发现可用 IP: {len(valid_ips)} 个")

        # 更新 DNS
        await update_cf_dns(valid_ips)

    duration = datetime.now() - start_time
    log(f"=== 任务完成，总耗时: {duration.total_seconds():.1f}秒 ===")

if __name__ == '__main__':
    asyncio.run(main())