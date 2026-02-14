import re
import json
import time
import os
import sys
import shutil
import zipfile
import asyncio
from urllib.parse import urljoin, urlparse
from playwright.async_api import async_playwright

# ================= 配置区域 =================
# 目标页面：米游社-绝区零-官方资讯
TARGET_URL = "https://www.miyoushe.com/zzz/home/58?type=3"

# 数据保存路径 (相对路径 - ZZZ_Miyoushe_Cloud_Download)
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
BASE_OUTPUT_DIR = os.path.join(PROJECT_ROOT, "ZZZ_Miyoushe_Cloud_Download")
DATA_DIR = os.path.join(BASE_OUTPUT_DIR, "data")
DOWNLOAD_ROOT = os.path.join(BASE_OUTPUT_DIR, "downloads")
OUTPUT_FILE = os.path.join(DATA_DIR, "scroll_spider_results.jsonl")
ERROR_LOG_FILE = os.path.join(BASE_OUTPUT_DIR, "spider_error.log")
FOLDER_MAP_FILE = os.path.join(DATA_DIR, "folder_map.json")

# 爬取配置
MAX_SCROLL_ATTEMPTS = 1000   # 最大滚动次数
SCROLL_PAUSE_TIME = 2.0      # 每次滚动后等待时间(秒)
NO_NEW_DATA_LIMIT = 5        # 连续N次滚动没有新内容则停止
HEADLESS = True             # 显示浏览器
MAX_PROCESS_LIMIT = 1000000  # 最大详情页处理数 (不限)
SLOW_MO = 100                # 操作延迟 (ms)
CONCURRENCY_LIMIT = 3        # 最大并发数 (多线程/多协程)

# ================= 全局锁 =================
file_write_lock = asyncio.Lock()
folder_map_lock = asyncio.Lock()
error_log_lock = asyncio.Lock()

# ================= 工具函数 =================
def ensure_dirs():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    if not os.path.exists(DOWNLOAD_ROOT):
        os.makedirs(DOWNLOAD_ROOT)

async def save_record(record):
    async with file_write_lock:
        with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

def sanitize_filename(name, max_length=80):
    """清理文件名/文件夹名"""
    name = re.sub(r'[\\/:*?"<>|]', '_', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name[:max_length]

async def handle_fatal_error(browser, url, context_info):
    """处理致命错误并记录日志"""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    log_content = (
        f"[{timestamp}] [FATAL ERROR] 404 Not Found detected.\n"
        f"Context: {context_info}\n"
        f"URL: {url}\n"
    )
    
    print(f"\n{'!'*60}")
    print(f"!!! 致命错误: 检测到 404 页面或无法访问的内容 !!!")
    print(f"!!! 发生位置: {context_info}")
    print(f"!!! 故障链接: {url}")
    print(f"!!! 详细日志已保存至: {ERROR_LOG_FILE}")
    print(f"!!! 程序即将停止以防止错误扩散。")
    print(f"{'!'*60}\n")
    
    async with error_log_lock:
        try:
            log_dir = os.path.dirname(ERROR_LOG_FILE)
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(log_content + "-"*60 + "\n")
        except Exception as e:
            print(f"Warning: Failed to write error log: {e}")
    
    if browser:
        try:
            await browser.close()
        except: pass
    
    # 强制退出
    sys.exit(1)

# ==============================================================================
# Helper: Folder Mapping Manager
# ==============================================================================
async def get_assigned_folder(cloud_url, suggested_name, root_dir):
    """
    根据云盘 URL 获取固定的本地文件夹路径。
    Thread-safe (via Lock) implementation.
    """
    async with folder_map_lock:
        # 1. 加载映射 (Sync IO is acceptable here for simplicity, inside lock)
        mapping = {}
        if os.path.exists(FOLDER_MAP_FILE):
            try:
                with open(FOLDER_MAP_FILE, 'r', encoding='utf-8') as f:
                    mapping = json.load(f)
            except: pass
        
        # 2. 检查是否已分配
        map_key = cloud_url
        if map_key in mapping:
            assigned_path = mapping[map_key]
            # 路径如果被手动删了，只是返回路径，后续负责创建
            return assigned_path
        
        # 3. 分配新路径
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
            
        # 4. 保存映射
        mapping[map_key] = final_path
        try:
            with open(FOLDER_MAP_FILE, 'w', encoding='utf-8') as f:
                json.dump(mapping, f, indent=2, ensure_ascii=False)
        except: pass
        
        return final_path

# ================= Playwright Helpers (Async) =================

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
        if await btn.is_visible() and ("zip" in (await btn.inner_text()).lower() or "打包" in (await btn.inner_text())):
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
            
            # 解压处理 (ZipFile is locking, run in executor if very large, but ok here)
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
        file_links = []
        for l in links:
            if await l.is_visible():
                txt = await l.inner_text()
                if txt.lower().endswith(valid_exts):
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

def extract_cloud_info_from_text(text):
    """从文本中提取云盘链接和密码"""
    pan_domains = [
        r"pan\.baidu\.com/s/[\w-]+", 
        r"yun\.baidu\.com/s/[\w-]+",
        r"aliyundrive\.com/s/[\w-]+",
        r"alipan\.com/s/[\w-]+",
        r"cloud\.189\.cn/t/[\w-]+",
        r"lanzou\w?\.com/[\w]+",
        r"quark\.cn/s/[\w-]+",
        r"123pan\.com/s/[\w-]+"
    ]
    
    found_links = []
    urls = re.findall(r"https?://[^\s\"')<>]+", text)
    minas_links = [u for u in urls if "minas.mihoyo.com" in u]
    other_links = []
    for u in urls:
        for domain_pat in pan_domains:
            if re.search(domain_pat, u):
                other_links.append(u)
                break
    
    codes = []
    code_patterns = [
        r"(?:密码|提取码|访问码|口令)\s*[:：]\s*([A-Za-z0-9]{4,})",
        r"(?:code)\s*[:：]\s*([A-Za-z0-9]{4,})"
    ]
    for pat in code_patterns:
        found = re.findall(pat, text)
        codes.extend(found)
        
    return list(set(minas_links + other_links)), list(set(codes))

# ================= 任务处理器 =================

async def process_article(context, browser_ref, article_url, title, semaphore):
    """单个文章的处理逻辑，由 semaphore 控制并发"""
    async with semaphore:
        print(f"  [Task] 开始处理: {title[:30]}...")
        worker_page = None
        try:
            worker_page = await context.new_page()
            
            # 访问详情页
            response = await worker_page.goto(article_url, wait_until="domcontentloaded", timeout=45000)
            if response and response.status == 404:
                await handle_fatal_error(browser_ref, article_url, f"Article Detail Page - Title: {title}")

            try:
                await worker_page.wait_for_load_state("networkidle", timeout=3000)
            except: pass
            
            content_html = await worker_page.content()
            content_text = await worker_page.inner_text("body")
            
            # 提取链接
            links_html, _ = extract_cloud_info_from_text(content_html)
            links_text, codes = extract_cloud_info_from_text(content_text)
            all_cloud_links = list(set(links_html + links_text))
            
            if not all_cloud_links:
                # print(f"    -> 无云盘链接: {title[:15]}...")
                return

            print(f"    -> {title[:15]}... 发现云盘链接: {len(all_cloud_links)} 个")
            
            for link in all_cloud_links:
                print(f"    --> 处理链接: {link}")
                cloud_page = None
                created_dir_path = None
                
                try:
                    # 模拟点击 / 新标签页打开
                    # 尝试寻找元素
                    try:
                        link_locator = worker_page.locator(f"a[href*='{link}']").first
                        if (await link_locator.count()) > 0 and (await link_locator.is_visible()):
                            print("      [Action] 模拟点击进入 (新标签页)...")
                            async with context.expect_page(timeout=10000) as new_page_info:
                                await worker_page.keyboard.down("Control")
                                await link_locator.click()
                                await worker_page.keyboard.up("Control")
                            cloud_page = await new_page_info.value
                            await cloud_page.wait_for_load_state("domcontentloaded")
                        else:
                            raise Exception("Element not found")
                    except Exception as e:
                        # 降级：直连
                        # print(f"      [Info] 元素查找失败: {e}, 转直连")
                        cloud_page = await context.new_page()
                        response = await cloud_page.goto(link, wait_until="domcontentloaded")
                        if response and response.status == 404:
                            await handle_fatal_error(browser_ref, link, "Cloud Disk Direct Access")

                    await asyncio.sleep(1)
                    
                    # 404 Check
                    if "404" in (await cloud_page.title()) or "页面不存在" in (await cloud_page.inner_text("body")):
                        await handle_fatal_error(browser_ref, link, "Cloud Disk Page 404 Check")

                    # Login
                    await attempt_cloud_login(cloud_page, codes)

                    # Folder Name
                    folder_name = await determine_local_folder(cloud_page, link)
                    
                    # Get/Assign Local Path (Thread Safe)
                    local_path = await get_assigned_folder(link, folder_name, DOWNLOAD_ROOT)
                    
                    if not os.path.exists(local_path):
                        os.makedirs(local_path)
                    created_dir_path = local_path
                    
                    print(f"    [Disk] 下载中: {local_path} FROM {title[:15]}")
                    
                    # Download
                    mode, files = await download_content(cloud_page, local_path)
                    
                    # Save Record
                    record = {
                        "title": title,
                        "article_url": article_url,
                        "cloud_url": link,
                        "local_path": local_path,
                        "files_downloaded": files,
                        "status": mode,
                        "time": time.strftime("%Y-%m-%d %H:%M:%S")
                    }
                    await save_record(record)
                    
                    # Cleanup
                    if not files and created_dir_path:
                        try:
                            if not os.listdir(created_dir_path):
                                os.rmdir(created_dir_path)
                        except: pass
                        
                except Exception as e:
                    print(f"    [Disk Error] {e} @ {link}")
                finally:
                    if cloud_page:
                        try: await cloud_page.close()
                        except: pass

        except Exception as e:
            print(f"    [Post Error] {title} 处理失败: {e}")
        finally:
            if worker_page:
                try: await worker_page.close()
                except: pass

async def run_spider_async():
    ensure_dirs()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO)
        
        # 1. 采集上下文 (不需要 user-agent/storage state 吗？默认即可)
        context = await browser.new_context(
            viewport={'width': 1280, 'height': 800},
            accept_downloads=True
        )
        
        page = await context.new_page()
        
        # === 阶段 1: 采集列表 (单线程) ===
        print(f"--> 打开页面: {TARGET_URL}")
        response = await page.goto(TARGET_URL, wait_until="domcontentloaded")
        if response and response.status == 404:
            await handle_fatal_error(browser, TARGET_URL, "Main Feed Page")
        await asyncio.sleep(3)
        
        print("--> 开始滚动采集列表...")
        last_item_count = 0
        no_change_counter = 0
        collected_links = set()
        
        for i in range(MAX_SCROLL_ATTEMPTS):
            elements = await page.locator("a[href*='/article/']").all()
            
            current_batch = set()
            for el in elements:
                try:
                    href = await el.get_attribute("href")
                    title = (await el.inner_text()).replace('\n', ' ').strip()
                    if href:
                        full_url = urljoin(TARGET_URL, href)
                        if "/article/" in full_url:
                            current_batch.add((full_url, title))
                except: continue
                
            for item in current_batch:
                collected_links.add(item)
                
            current_count = len(collected_links)
            print(f"    [Scroll {i+1}] 当前捕获文章数: {current_count}")
            
            if current_count > last_item_count:
                last_item_count = current_count
                no_change_counter = 0
            else:
                no_change_counter += 1
                
            if no_change_counter >= NO_NEW_DATA_LIMIT:
                print("    -> 连续多次未发现新文章，停止滚动。")
                break
            
            # Scroll
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            try:
                await asyncio.sleep(SCROLL_PAUSE_TIME)
            except: pass
            
        print(f"--> 列表采集完成，共 {len(collected_links)} 篇文章。")
        await page.close() # 关闭列表页，释放资源
        
        # === 阶段 2: 多线程/多协程 处理 ===
        print(f"--> 开始并发处理任务 (并发数: {CONCURRENCY_LIMIT})...")
        
        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        tasks = []
        
        # 转换为列表以便切片限制
        all_items = list(collected_links)
        if MAX_PROCESS_LIMIT and MAX_PROCESS_LIMIT < len(all_items):
            all_items = all_items[:MAX_PROCESS_LIMIT]
            
        for idx, (url, title) in enumerate(all_items):
             task = asyncio.create_task(process_article(context, browser, url, title, semaphore))
             tasks.append(task)
             
        # 等待所有任务
        if tasks:
            await asyncio.gather(*tasks)
            
        print(f"--> 全部完成，结果已保存至: {OUTPUT_FILE}")
        await browser.close()

if __name__ == "__main__":
    if sys.platform == 'win32':
        # 设置 Windows 下的 event loop policy，防止 playwright 报错
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        
    try:
        asyncio.run(run_spider_async())
    except KeyboardInterrupt:
        print("\n[Stop] 用户终止程序")
    except Exception as e:
        print(f"\n[Error] 程序异常退出: {e}")
