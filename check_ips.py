import asyncio
import aiohttp
import os

# 配置
URL_LIST_FILE = 'url.txt'    # 存放订阅源 URL 的文件
OUTPUT_FILE = 'valid_ips.txt' # 存放检测成功的 IP
CHECK_API = 'https://api.090227.xyz/check'
CONCURRENT_LIMIT = 50        # 建议保持在 50 左右，避免被接口屏蔽

async def fetch_ips_from_url(session, url):
    """获取远程 IP 列表"""
    try:
        async with session.get(url, timeout=15) as resp:
            if resp.status == 200:
                text = await resp.text()
                # 按行切分，去除每行首尾空格，并过滤空行
                ips = [line.strip() for line in text.splitlines() if line.strip()]
                print(f"[Fetch] 从 {url} 获取到 {len(ips)} 个 IP")
                return ips
            else:
                print(f"[Error] 请求失败: {url} (状态码: {resp.status})")
    except Exception as e:
        print(f"[Error] 访问异常 {url}: {e}")
    return []

async def check_proxy(session, ip, semaphore):
    """并行检测 IP"""
    async with semaphore:
        # 直接拼接 IP，不加端口
        params = {'proxyip': ip}
        try:
            async with session.get(CHECK_API, params=params, timeout=12) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    # 只要 API 判定 success 为真即通过
                    if data.get('success') is True:
                        print(f"[OK] {ip}")
                        return ip
        except:
            pass
        return None

async def main():
    if not os.path.exists(URL_LIST_FILE):
        print(f"错误: 找不到 {URL_LIST_FILE} 文件")
        return

    async with aiohttp.ClientSession() as session:
        # 1. 读取订阅源 URL 列表
        with open(URL_LIST_FILE, 'r', encoding='utf-8') as f:
            urls = [line.strip() for line in f if line.strip()]
        
        if not urls:
            print("url.txt 为空，请填入有效的远程 IP 列表地址")
            return

        # 2. 抓取所有 IP 并汇总去重
        print(f"正在从 {len(urls)} 个源获取数据...")
        fetch_tasks = [fetch_ips_from_url(session, url) for url in urls]
        results_list = await asyncio.gather(*fetch_tasks)
        
        # 将嵌套列表扁平化并去重
        all_ips = list(set([ip for sublist in results_list for ip in sublist]))
        
        if not all_ips:
            print("未获取到任何 IP 数据。")
            return

        print(f"汇总完成，共计 {len(all_ips)} 个唯一 IP。开始并行检测...")

        # 3. 限制并发并执行检测
        semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)
        check_tasks = [check_proxy(session, ip, semaphore) for ip in all_ips]
        final_results = await asyncio.gather(*check_tasks)

    # 4. 过滤有效结果并保存
    valid_ips = [r for r in final_results if r]
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write('\n'.join(valid_ips))
    
    print(f"\n--- 检测报告 ---")
    print(f"扫描总数: {len(all_ips)}")
    print(f"成功数量: {len(valid_ips)}")
    print(f"结果文件: {OUTPUT_FILE}")

if __name__ == '__main__':
    asyncio.run(main())