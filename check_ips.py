import asyncio
import aiohttp
import os
import re

# 配置
URL_LIST_FILE = 'url.txt'    # 存放 URL 订阅地址的文件
OUTPUT_FILE = 'valid_ips.txt' # 产出的有效 IP 文件
CHECK_API = 'https://api.090227.xyz/check'
CONCURRENT_LIMIT = 50        # 并发限制

async def fetch_ips_from_url(session, url):
    """从单个 URL 获取 IP 列表文本"""
    try:
        async with session.get(url, timeout=15) as resp:
            if resp.status == 200:
                text = await resp.text()
                # 使用正则提取类似 IP:PORT 的字符串，兼容不同格式
                found = re.findall(r'(?:[\d]{1,3}\.){3}[\d]{1,3}:[\d]+', text)
                print(f"[Fetch] 从 {url} 获取到 {len(found)} 个 IP")
                return found
    except Exception as e:
        print(f"[Error] 无法访问 URL {url}: {e}")
    return []

async def check_proxy(session, proxy_ip, semaphore):
    """检测单个 IP 是否有效"""
    async with semaphore:
        url = f"{CHECK_API}?proxyip={proxy_ip.strip()}"
        try:
            async with session.get(url, timeout=12) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('success'):
                        return proxy_ip.strip()
        except:
            pass
        return None

async def main():
    if not os.path.exists(URL_LIST_FILE):
        print(f"找不到 {URL_LIST_FILE}")
        return

    async with aiohttp.ClientSession() as session:
        # 第一步：读取 url.txt 并抓取所有 IP
        with open(URL_LIST_FILE, 'r') as f:
            urls = [line.strip() for line in f if line.strip()]
        
        print(f"正在从 {len(urls)} 个源抓取 IP...")
        fetch_tasks = [fetch_ips_from_url(session, url) for url in urls]
        ip_lists = await asyncio.gather(*fetch_tasks)
        
        # 汇总并去重
        all_ips = list(set([ip for sublist in ip_lists for ip in sublist]))
        print(f"抓取完成，共计 {len(all_ips)} 个唯一 IP。开始检测...")

        # 第二步：并行检测
        semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)
        check_tasks = [check_proxy(session, ip, semaphore) for ip in all_ips]
        results = await asyncio.gather(*check_tasks)

    # 保存结果
    valid_ips = [r for r in results if r]
    with open(OUTPUT_FILE, 'w') as f:
        f.write('\n'.join(valid_ips))
    
    print(f"\n--- 检测报告 ---")
    print(f"总计扫描: {len(all_ips)}")
    print(f"有效数量: {len(valid_ips)}")
    print(f"结果已保存至: {OUTPUT_FILE}")

if __name__ == '__main__':
    asyncio.run(main())
