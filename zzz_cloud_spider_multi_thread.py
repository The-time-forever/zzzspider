import re
import json
import os
import time
import zipfile
import asyncio
from urllib.parse import urljoin, urlparse
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ================= 配置区域 =================
# 是否无头模式 (User requested True, and original was False but user asked to not popup browser)
HEADLESS = True
# 操作减速 (毫秒)
SLOW_MO = 100
# 目录页入口
CATALOG_URL = "https://zzz.mihoyo.com/news?utm_source=oolandingpage"
# 基础数据存目录
DATA_DIR = "d:/Users/22542/Desktop/zzzspider/data"
# 下载保存目录
DOWNLOAD_ROOT = "d:/Users/22542/Desktop/zzzspider/downloads"
# 最大并发任务数 (建议 3-5，过高会导致内存/CPU 压力大或被封禁)
CONCURRENCY_LIMIT = 3
# 最大处理新闻数 (设置为 None 则处理所有采集到的)
MAX_NEWS_LIMIT = None 
# ===========================================

# 确保目录存在
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)
if not os.path.exists(DOWNLOAD_ROOT):
    os.makedirs(DOWNLOAD_ROOT)

def sanitize_filename(name, max_length=80):
    """清理文件名/文件夹名"""
    name = re.sub(r'[\\/:*?"<>|]', '_', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name[:max_length]

# 文件写入锁，防止多协程同时写入损坏文件
file_lock = asyncio.Lock()

async def safe_write_json(filepath, data):
    """线程/协程安全的文件写入"""
    async with file_lock:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

# ==============================================================================
# Part 1: 详情页处理器 (Async版)
# ==============================================================================

def extract_from_text(text):
    """从文本中提取云盘链接和密码"""
    all_urls = re.findall(r"https?://[^\s)\"'>]+", text)
    cloud_links = []
    seen = set()
    for link in all_urls:
        if "minas.mihoyo.com" in link and link not in seen:
            cloud_links.append(link)
            seen.add(link)
    
    pwd_pattern = re.compile(r"(?:密码|提取码)\s*[:：]\s*([A-Za-z0-9_-]{4,})")
    passwords = []
    seen_pwds = set()
    for match in pwd_pattern.findall(text):
        if match not in seen_pwds:
            passwords.append(match)
            seen_pwds.add(match)
    return cloud_links, passwords

async def attempt_cloud_login(page, password_candidates):
    """尝试云盘登录"""
    input_selectors = ["input[type='password']", "input[placeholder*='密码']", "input[placeholder*='提取']"]
    confirm_selectors = ["button:has-text('确认')", "button:has-text('确定')", "button:has-text('进入')"]

    found_input = None
    for sel in input_selectors:
        if await page.locator(sel).is_visible():
            found_input = sel
            break
            
    if not found_input:
        return None  # 无需密码

    print(f"      [Login] 发现密码框，开始尝试...")
    for pwd in password_candidates:
        try:
            await page.fill(found_input, pwd)
            clicked = False
            for btn in confirm_selectors:
                if await page.locator(btn).is_visible():
                    await page.click(btn)
                    clicked = True
                    break
            if not clicked:
                await page.press(found_input, "Enter")
            
            await asyncio.sleep(1.5)
            if not await page.locator(found_input).is_visible():
                return pwd
        except:
            pass
    return None

async def determine_local_folder(page, url):
    """确定本地文件夹名"""
    folder_name = ""
    try:
        # 尝试从面包屑或其他位置获取
        elements = await page.get_by_text(re.compile("当前路径|位置|Path")).all()
        for el in elements:
            if await el.is_visible():
                txt = (await el.inner_text()).strip()
                if 5 < len(txt) < 100:
                    folder_name = txt
                    break
    except: pass
    
    if not folder_name:
        title = (await page.title()).strip()
        if title and "mihoyo" not in title.lower():
            folder_name = title
        else:
            folder_name = url.rstrip('/').split('/')[-1] or f"disk_{int(time.time())}"
            
    return sanitize_filename(folder_name)

async def download_content(page, local_dir):
    """核心下载逻辑：优先ZIP，降级逐个文件"""
    downloaded_files = []
    mode = "failed"
    
    # 1. 尝试 ZIP
    zip_btns = await page.locator("button, a").filter(has_text=re.compile("ZIP|打包|全部下载", re.IGNORECASE)).all()
    target_btn = None
    for btn in zip_btns:
        inner_text = await btn.inner_text()
        if await btn.is_visible() and ("zip" in inner_text.lower() or "打包" in inner_text):
            target_btn = btn
            break
            
    if target_btn:
        print(f"      [ZIP] 发现打包下载按钮，尝试下载...")
        try:
            async with page.expect_download(timeout=60000) as download_info:
                await target_btn.click()
            
            download = await download_info.value
            safe_name = sanitize_filename(download.suggested_filename)
            save_path = os.path.join(local_dir, safe_name)
            await download.save_as(save_path)
            
            # 解压处理 (IO密集型，放到线程池里避免阻塞 Loop)
            # 但这里为了简单，且 zip 操作可能较快，暂且同步执行
            if zipfile.is_zipfile(save_path):
                try:
                    with zipfile.ZipFile(save_path, 'r') as zf:
                        zf.extractall(local_dir)
                        downloaded_files.extend(zf.namelist())
                    os.remove(save_path) # 删除原 ZIP
                    mode = "zip_extracted"
                except Exception as e:
                    print(f"      [ZIP] 解压失败: {e}")
                    downloaded_files.append(safe_name)
                    mode = "zip_raw"
            else:
                downloaded_files.append(safe_name)
                mode = "zip_file"
            return mode, downloaded_files
        except Exception as e:
            print(f"      [ZIP] 流程异常: {e}, 转为逐个下载...")

    # 2. 降级：逐个文件
    print("      [Fallback] 尝试逐个文件下载...")
    valid_exts = ('.jpg', '.png', '.gif', '.zip', '.rar', '.7z', '.mp4')
    try:
        links = await page.locator("a[href]").all()
        # 需异步过滤
        file_links = []
        for l in links:
             if await l.is_visible():
                 txt = (await l.inner_text()).lower()
                 if txt.endswith(valid_exts):
                     file_links.append(l)
        
        if not file_links:
            return "no_files_found", []

        for idx, link in enumerate(file_links):
            fname = (await link.inner_text()).strip()
            safe_fname = sanitize_filename(fname)
            
            # 检查文件是否已存在 (去重)
            if os.path.exists(os.path.join(local_dir, safe_fname)):
                print(f"      [Skip] 文件已存在: {safe_fname}")
                downloaded_files.append(safe_fname)
                continue

            # 重试逻辑
            for attempt in range(2):
                try:
                    async with page.expect_download(timeout=15000) as di:
                        await link.click(timeout=3000)
                    dl = await di.value
                    sname = sanitize_filename(dl.suggested_filename) or safe_fname
                    await dl.save_as(os.path.join(local_dir, sname))
                    downloaded_files.append(sname)
                    await asyncio.sleep(0.5)
                    break
                except:
                    if attempt == 0: await asyncio.sleep(1)
        
        if downloaded_files:
            mode = "individual_files"
    except Exception as e:
        print(f"      [Fallback] 异常: {e}")
        
    return mode, downloaded_files

# ==============================================================================
# Helper: Folder Mapping Manager
# ==============================================================================
FOLDER_MAP_FILE = os.path.join(DATA_DIR, "folder_map.json")

async def get_assigned_folder_async(cloud_url, suggested_name, root_dir):
    """
    根据云盘 URL 获取固定的本地文件夹路径。
    Async 版本的封装，主要是为了加锁读取/写入，防止多线程竞争文件。
    """
    async with file_lock:
        mapping = {}
        if os.path.exists(FOLDER_MAP_FILE):
            try:
                with open(FOLDER_MAP_FILE, 'r', encoding='utf-8') as f:
                    mapping = json.load(f)
            except: pass
        
        map_key = cloud_url
        
        if map_key in mapping:
            assigned_path = mapping[map_key]
            return assigned_path
        
        base_path = os.path.join(root_dir, suggested_name)
        final_path = base_path
        
        used_paths = set(p.lower().replace('\\', '/') for p in mapping.values())
        
        counter = 1
        while True:
            check_path_norm = final_path.lower().replace('\\', '/')
            is_physically_exists = os.path.exists(final_path) and os.listdir(final_path)
            is_reserved = check_path_norm in used_paths
            
            if not is_physically_exists and not is_reserved:
                break
                
            final_path = f"{base_path}_{counter:02d}"
            counter += 1
            
        mapping[map_key] = final_path
        try:
            with open(FOLDER_MAP_FILE, 'w', encoding='utf-8') as f:
                json.dump(mapping, f, indent=2, ensure_ascii=False)
        except: pass
        
        return final_path

async def process_news_detail(context, news_url, output_root, processed_set, full_results, processed_file, results_file):
    """处理单个新闻详情页 (Async)"""
    result = {
        "news_url": news_url,
        "cloud_links_found": [],
        "processed_disks": [],
        "status": "success",
        "error_msg": ""
    }
    
    page = await context.new_page()
    
    try:
        print(f"  > [Detail] 打开新闻页: {news_url}")
        await page.goto(news_url, wait_until="domcontentloaded", timeout=45000)
        try:
            await page.wait_for_load_state("networkidle", timeout=5000)
        except: pass
        
        text = await page.inner_text("body")
        cloud_links, pwds = extract_from_text(text)
        result["cloud_links_found"] = cloud_links
        
        if not cloud_links:
            print(f"    -> [{news_url}] 无云盘链接")
        else:
            print(f"    -> [{news_url}] 找到 {len(cloud_links)} 个云盘链接")
            
            for link in cloud_links:
                disk_res = {
                    "url": link, 
                    "pwd": None, 
                    "mode": "pending", 
                    "local_folder": None,
                    "files": []
                }
                
                created_dir_path = None

                try:
                    await page.goto(link, wait_until="domcontentloaded", timeout=45000)
                    await asyncio.sleep(1)
                    
                    used_pwd = await attempt_cloud_login(page, pwds)
                    disk_res["pwd"] = used_pwd
                    
                    try:
                        await page.wait_for_load_state("networkidle", timeout=5000)
                    except: pass
                    
                    folder_name = await determine_local_folder(page, link)
                    local_path = await get_assigned_folder_async(link, folder_name, output_root)
                    
                    if not os.path.exists(local_path):
                        os.makedirs(local_path)
                    created_dir_path = local_path
                    
                    disk_res["local_folder"] = local_path
                    print(f"    -> [Disk] 下载到: {local_path}")
                    
                    mode, files = await download_content(page, local_path)
                    disk_res["mode"] = mode
                    disk_res["files"] = files

                    if not files and created_dir_path:
                        try:
                            if not os.listdir(created_dir_path):
                                os.rmdir(created_dir_path)
                                print(f"    -> [Cleanup] 空目录已删除: {created_dir_path}")
                        except Exception as clean_err:
                            print(f"    -> [Cleanup Warn] {clean_err}")
                    
                except Exception as e:
                    print(f"    -> [Disk Error] {e}")
                    disk_res["error"] = str(e)
                
                result["processed_disks"].append(disk_res)
            
    except Exception as e:
        result["status"] = "error"
        result["error_msg"] = str(e)
        print(f"  > [Detail Error] {news_url}: {e}")
    finally:
        await page.close()
        
    # 保存结果 (使用锁)
    async with file_lock:
        processed_set.add(news_url)
        full_results.append(result)
        
        # 写入文件
        # 为了不频繁IO，可以考虑批量写，这里为了安全起见依然实时写
        try:
            with open(processed_file, "w", encoding="utf-8") as f:
                json.dump(list(processed_set), f, indent=2)
            with open(results_file, "w", encoding="utf-8") as f:
                json.dump(full_results, f, indent=2, ensure_ascii=False)
        except: pass

    return result

# ==============================================================================
# Part 2: 目录页采集器 (保持逻辑复刻 Async 版)
# ==============================================================================

async def collect_news_urls(page, catalog_url):
    """
    采集所有新闻链接 (支持分页 + URL 归一化)
    """
    collected_urls = set()
    tabs = ["最新"]
    
    print(f"--> [Collector] 访问目录页: {catalog_url}")
    try:
        await page.goto(catalog_url, wait_until="networkidle", timeout=60000)
    except:
        await page.goto(catalog_url, wait_until="load", timeout=60000)

    async def extract_current_page_links():
        s = set()
        try:
            links = await page.locator("a[href]").all()
            for link in links:
                href = await link.get_attribute("href")
                if not href: continue
                
                full_url = urljoin("https://zzz.mihoyo.com", href)
                parsed = urlparse(full_url)
                path = parsed.path
                
                if re.match(r"^/news/\d+$", path):
                    clean_url = f"{parsed.scheme}://{parsed.netloc}{path}"
                    s.add(clean_url)
        except Exception as e:
            print(f"      [Scan Warn] 链接提取部分失败: {e}")
        return s

    for tab_name in tabs:
        print(f"\n--> [Collector] 切换 Tab: {tab_name}")
        try:
            tab_locator = page.get_by_text(tab_name, exact=True)
            if await tab_locator.count() > 0 and await tab_locator.first.is_visible():
                await tab_locator.first.click()
                await asyncio.sleep(1.5)
            else:
                print(f"    [Warn] Tab '{tab_name}' 未找到，跳过。")
                continue
            
            try:
                page1_btn = page.locator("a.mihoyo-pager-rich__button").filter(has_text="1")
                if await page1_btn.count() > 0 and await page1_btn.first.is_visible():
                    current_chk = page.locator("a.mihoyo-pager-rich__button.mihoyo-pager-rich__current")
                    if await current_chk.count() > 0 and (await current_chk.first.inner_text()).strip() != "1":
                        print("    [Reset] 点击回到第 1 页...")
                        await page1_btn.first.click()
                        await asyncio.sleep(1.5)
            except Exception as e:
                print(f"    [Reset Warn] 回到第一页尝试失败: {e}")

        except Exception as e:
            print(f"    [Tab Error] {tab_name}: {e}")
            continue

        page_index = 1
        max_page_per_tab = 200
        empty_page_count = 0 
        
        while page_index <= max_page_per_tab:
            current_page_urls = await extract_current_page_links()
            
            new_added = 0
            for u in current_page_urls:
                if u not in collected_urls:
                    collected_urls.add(u)
                    new_added += 1
            
            try:
                curr_el = page.locator("a.mihoyo-pager-rich__button.mihoyo-pager-rich__current").first
                curr_txt = (await curr_el.inner_text()).strip() if await curr_el.count() > 0 else str(page_index)
            except:
                curr_txt = str(page_index)

            print(f"    [Tab:{tab_name}] [Page {curr_txt}] 本页识别: {len(current_page_urls)} 条 | 新增: {new_added} | 总计: {len(collected_urls)}")

            if len(current_page_urls) == 0:
                empty_page_count += 1
                if empty_page_count >= 3:
                     print("    -> 连续多次未获取到链接，判定为 Tab 结束。")
                     break
            else:
                empty_page_count = 0

            try:
                current_active = page.locator("a.mihoyo-pager-rich__button.mihoyo-pager-rich__current").first
                if not await current_active.is_visible():
                    print("    [Pagination] 未找到当前页高亮元素，可能只有一页或结构变化。")
                    break
                
                old_page_num = (await current_active.inner_text()).strip()
                
                next_page_btn = current_active.locator("xpath=following-sibling::a[contains(@class,'mihoyo-pager-rich__button')][1]")
                
                if await next_page_btn.count() > 0 and await next_page_btn.is_visible():
                    await next_page_btn.click()
                    
                    page_changed = False
                    for _ in range(20): 
                        await asyncio.sleep(0.5)
                        try:
                            check_curr = page.locator("a.mihoyo-pager-rich__button.mihoyo-pager-rich__current").first
                            if await check_curr.is_visible() and (await check_curr.inner_text()).strip() != old_page_num:
                                page_changed = True
                                break
                        except: pass
                    
                    if not page_changed:
                         print(f"    [Pagination] 翻页超时 (当前页码 {old_page_num} 未变)，可能已是末页或卡住。")
                         break
                    else:
                        page_index += 1
                        try:
                            await page.wait_for_load_state("networkidle", timeout=2000)
                        except: pass

                else:
                    print("    [Pagination] 没有下一个页码按钮了，本 Tab 结束。")
                    break

            except Exception as e:
                print(f"    [Pagination] 翻页异常: {e}")
                break
                
    final_list = sorted(list(collected_urls), reverse=True)
    return final_list

# ==============================================================================
# Part 3: 主控逻辑
# ==============================================================================

async def task_runner(sem, context, url, output_root, processed_set, full_results, processed_file, results_file):
    """
    带信号量的任务包装器
    """
    async with sem:
        await process_news_detail(context, url, output_root, processed_set, full_results, processed_file, results_file)

async def main():
    print("=== 全站采集脚本(多线程异步版) 启动 ===")
    
    processed_file = os.path.join(DATA_DIR, "processed_news.json")
    results_file = os.path.join(DATA_DIR, "results.json")
    news_urls_file = os.path.join(DATA_DIR, "news_urls.json")
    
    processed_set = set()
    if os.path.exists(processed_file):
        try:
            with open(processed_file, "r", encoding="utf-8") as f:
                processed_set = set(json.load(f))
        except: pass
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS, 
            slow_mo=SLOW_MO,
            args=["--start-maximized"]
        )
        # 创建 Context
        context = await browser.new_context(
            accept_downloads=True,
            viewport={'width': 1920, 'height': 1080}
        )
        
        # 1. 采集目录 (单线程采集，因为翻页依赖上下文)
        page = await context.new_page()
        try:
            all_news_urls = await collect_news_urls(page, CATALOG_URL)
        except Exception as e:
            print(f"采集出错: {e}")
            all_news_urls = []
        await page.close()
        
        with open(news_urls_file, "w", encoding="utf-8") as f:
            json.dump(all_news_urls, f, indent=2)
            
        print(f"\n--> 采集完成，共 {len(all_news_urls)} 个链接")
        
        # 2. 准备任务队列
        tasks_to_run = []
        for url in all_news_urls:
            if url not in processed_set:
                tasks_to_run.append(url)
        
        print(f"--> 需要处理的任务: {len(tasks_to_run)} (已跳过 {len(processed_set)} 个)")
        
        if MAX_NEWS_LIMIT:
            print(f"    (测试模式) 仅处理前 {MAX_NEWS_LIMIT} 个")
            tasks_to_run = tasks_to_run[:MAX_NEWS_LIMIT]

        full_results = []
        if os.path.exists(results_file):
            try:
                with open(results_file, "r", encoding="utf-8") as f:
                    full_results = json.load(f)
            except: pass

        # 3. 并发执行
        # 使用 Semaphore 限制并发数
        sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
        
        print(f"--> 开始并发处理，并发数限制: {CONCURRENCY_LIMIT}")
        
        await_tasks = []
        for url in tasks_to_run:
            t = asyncio.create_task(
                task_runner(sem, context, url, DOWNLOAD_ROOT, processed_set, full_results, processed_file, results_file)
            )
            await_tasks.append(t)
            
        if await_tasks:
            # gather 会等待所有任务完成
            await asyncio.gather(*await_tasks)
        
        print("\n=== 全部任务结束 ===")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
