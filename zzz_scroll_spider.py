import re
import json
import time
import os
import sys
import shutil
import zipfile
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright

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

# 爬取配置
MAX_SCROLL_ATTEMPTS = 1000  # 最大滚动次数 (增加以获取更多数据)
SCROLL_PAUSE_TIME = 2.0    # 每次滚动后等待时间(秒)
NO_NEW_DATA_LIMIT = 5      # 连续N次滚动没有新内容则停止
HEADLESS = False           # 显示浏览器以便观察滚动效果
MAX_PROCESS_LIMIT = 5000   # 最大详情页处理数 (不限数量)
SLOW_MO = 100              # 下载时的操作延迟

# ================= 工具函数 =================
def ensure_dirs():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    if not os.path.exists(DOWNLOAD_ROOT):
        os.makedirs(DOWNLOAD_ROOT)

def save_record(record):
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

def sanitize_filename(name, max_length=80):
    """清理文件名/文件夹名"""
    name = re.sub(r'[\\/:*?"<>|]', '_', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name[:max_length]

def handle_fatal_error(browser, url, context_info):
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
    print(f"!!! 程序已紧急停止以防止错误扩散。")
    print(f"{'!'*60}\n")
    
    try:
        # 确保目录存在
        log_dir = os.path.dirname(ERROR_LOG_FILE)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(log_content + "-"*60 + "\n")
    except Exception as e:
        print(f"Warning: Failed to write error log: {e}")
    
    if browser:
        try:
            browser.close()
        except: pass
    sys.exit(1)

# ==============================================================================
# Helper: Folder Mapping Manager
# ==============================================================================
FOLDER_MAP_FILE = os.path.join(DATA_DIR, "folder_map.json")

def get_assigned_folder(cloud_url, suggested_name, root_dir):
    """
    根据云盘 URL 获取固定的本地文件夹路径。
    如果已存在映射，则复用；否则分配新名（处理重名）并保存映射。
    """
    # 1. 加载映射
    mapping = {}
    if os.path.exists(FOLDER_MAP_FILE):
        try:
            with open(FOLDER_MAP_FILE, 'r', encoding='utf-8') as f:
                mapping = json.load(f)
        except: pass
    
    # 2. 检查是否已分配
    # 使用 url 的 path 部分作为 key 避免 query 参数干扰
    map_key = cloud_url
    
    if map_key in mapping:
        assigned_path = mapping[map_key]
        if not os.path.exists(assigned_path):
             # 路径如果被手动删了，也需要创建父级
             parent = os.path.dirname(assigned_path)
        return assigned_path
    
    # 3. 分配新路径
    base_path = os.path.join(root_dir, suggested_name)
    final_path = base_path
    
    # 获取所有已经被占用的路径集合
    used_paths = set(p.lower().replace('\\', '/') for p in mapping.values())
    
    counter = 1
    # 冲突检测：路径物理存在 OR 路径已被其他 URL 预占
    while True:
        check_path_norm = final_path.lower().replace('\\', '/')
        is_physically_exists = os.path.exists(final_path) and os.listdir(final_path) # 存在且非空
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

def attempt_cloud_login(page, password_candidates):
    """尝试云盘登录"""
    input_selectors = ["input[type='password']", "input[placeholder*='密码']", "input[placeholder*='提取']"]
    confirm_selectors = ["button:has-text('确认')", "button:has-text('确定')", "button:has-text('进入')"]

    found_input = None
    for sel in input_selectors:
        if page.locator(sel).is_visible():
            found_input = sel
            break
            
    if not found_input:
        return None  # 无需密码

    print(f"      [Login] 发现密码框，开始尝试...")
    for pwd in password_candidates:
        try:
            page.fill(found_input, pwd)
            clicked = False
            for btn in confirm_selectors:
                if page.locator(btn).is_visible():
                    page.click(btn)
                    clicked = True
                    break
            if not clicked:
                page.press(found_input, "Enter")
            
            time.sleep(1.5)
            if not page.locator(found_input).is_visible():
                return pwd
        except:
            pass
    return None

def determine_local_folder(page, url):
    """确定本地文件夹名"""
    folder_name = ""
    try:
        # 尝试从面包屑或其他位置获取
        elements = page.get_by_text(re.compile("当前路径|位置|Path")).all()
        for el in elements:
            if el.is_visible():
                txt = el.inner_text().strip()
                if 5 < len(txt) < 100:
                    folder_name = txt
                    break
    except: pass
    
    if not folder_name:
        title = page.title().strip()
        if title and "mihoyo" not in title.lower():
            folder_name = title
        else:
            folder_name = url.rstrip('/').split('/')[-1] or f"disk_{int(time.time())}"
            
    return sanitize_filename(folder_name)

def download_content(page, local_dir):
    """核心下载逻辑：优先ZIP，降级逐个文件"""
    downloaded_files = []
    mode = "failed"
    
    # 1. 尝试 ZIP
    zip_btns = page.locator("button, a").filter(has_text=re.compile("ZIP|打包|全部下载", re.IGNORECASE)).all()
    target_btn = None
    for btn in zip_btns:
        if btn.is_visible() and ("zip" in btn.inner_text().lower() or "打包" in btn.inner_text()):
            target_btn = btn
            break
            
    if target_btn:
        print(f"      [ZIP] 发现打包下载按钮，尝试下载...")
        try:
            with page.expect_download(timeout=60000) as download_info:
                target_btn.click()
            
            download = download_info.value
            safe_name = sanitize_filename(download.suggested_filename)
            save_path = os.path.join(local_dir, safe_name)
            download.save_as(save_path)
            
            # 解压处理
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
        links = page.locator("a[href]").all()
        file_links = [l for l in links if l.is_visible() and l.inner_text().lower().endswith(valid_exts)]
        
        if not file_links:
            return "no_files_found", []

        for idx, link in enumerate(file_links):
            fname = link.inner_text().strip()
            safe_fname = sanitize_filename(fname)
            
            # 检查文件是否已存在 (去重)
            if os.path.exists(os.path.join(local_dir, safe_fname)):
                print(f"      [Skip] 文件已存在: {safe_fname}")
                downloaded_files.append(safe_fname)
                continue

            # 重试逻辑
            for attempt in range(2):
                try:
                    with page.expect_download(timeout=15000) as di:
                        link.click(timeout=3000)
                    dl = di.value
                    sname = sanitize_filename(dl.suggested_filename) or safe_fname
                    dl.save_as(os.path.join(local_dir, sname))
                    downloaded_files.append(sname)
                    time.sleep(0.5)
                    break
                except:
                    if attempt == 0: time.sleep(1)
        
        if downloaded_files:
            mode = "individual_files"
    except Exception as e:
        print(f"      [Fallback] 异常: {e}")
        
    return mode, downloaded_files


# ================= 核心逻辑 =================

def extract_cloud_info_from_text(text):
    """从文本中提取云盘链接和密码"""
    # 常见网盘域名
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
    # 1. 简单正则提取 URL
    urls = re.findall(r"https?://[^\s\"')<>]+", text)
    # 优先匹配 minas (米哈游专用)
    minas_links = [u for u in urls if "minas.mihoyo.com" in u]
    
    # 其他网盘
    other_links = []
    for u in urls:
        for domain_pat in pan_domains:
            if re.search(domain_pat, u):
                other_links.append(u)
                break
    
    # 2. 提取密码/提取码
    codes = []
    code_patterns = [
        r"(?:密码|提取码|访问码|口令)\s*[:：]\s*([A-Za-z0-9]{4,})",
        r"(?:code)\s*[:：]\s*([A-Za-z0-9]{4,})"
    ]
    for pat in code_patterns:
        found = re.findall(pat, text)
        codes.extend(found)
        
    # 合并去重
    return list(set(minas_links + other_links)), list(set(codes))

def process_single_article(context, browser, article_url, title):
    """(Refactored) 处理单个详情页，包含提取云盘链接和下载"""
    worker_page = None
    try:
        worker_page = context.new_page()
        print(f"  [Processing] 分析: {title[:30]}...")
        
        # 访问详情页
        response = worker_page.goto(article_url, wait_until="domcontentloaded", timeout=45000)
        if response and response.status == 404:
            handle_fatal_error(browser, article_url, f"Article Detail Page (文章详情) - Title: {title}")

        # === 新增: Soft 404 检测 (针对 HTTP 200 但内容错误的页面) ===
        try:
            page_title = worker_page.title()
            page_text_start = worker_page.inner_text("body")[:500] # 只取前500字符快速检查
            
            # 米游社/常见错误特征
            # 补充截图中的特定文案："偏离了地球"
            error_keywords = [
                "页面丢失", "404", "帖子不存在", "文章不存在", "系统繁忙", 
                "偏离了地球", "404 Not Found", "该内容已被隐藏"
            ]
            is_soft_404 = any(k in page_title for k in error_keywords) or \
                          any(k in page_text_start for k in error_keywords)
            
            # 二次确认: 有些 404 页面标题正常且文字很少，尝试检测特定元素
            if not is_soft_404:
                # 检查是否存在那个经典的 404 图片或容器 class (通常包含 404 字眼)
                # 截图中的 404 往往有特定的 class 或者是特定的 img alt
                try:
                    # 尝试检测页面内是否有明显的 404 大字节点
                    if worker_page.locator("text=404").count() > 0:
                        is_soft_404 = True
                    # 或检测包含 "偏离了地球" 的元素
                    elif worker_page.get_by_text("偏离了地球").count() > 0:
                        is_soft_404 = True
                except: pass

            if is_soft_404:
                handle_fatal_error(browser, article_url, f"Article Detail Page (Soft 404 Detected) - Page Title: {page_title}")
        except Exception: 
            pass # 页面可能还没渲染完，或者是非致命错误，继续往下走

        try:
            worker_page.wait_for_load_state("networkidle", timeout=3000)
        except: pass
        
        content_html = worker_page.content()
        content_text = worker_page.inner_text("body")
        
        # 提取链接
        links_html, _ = extract_cloud_info_from_text(content_html)
        links_text, codes = extract_cloud_info_from_text(content_text)
        all_cloud_links = list(set(links_html + links_text))
        
        if not all_cloud_links:
             # print("    -> 无云盘链接")
             return

        print(f"    -> 发现云盘链接: {len(all_cloud_links)} 个")
        
        # 开始下载流程
        for link in all_cloud_links:
            print(f"    --> 处理链接: {link}")
            
            cloud_page = None
            created_dir_path = None
            try:
                # 尝试寻找页面上的对应链接元素并点击 (Ctrl+Click 强制新标签页)
                # 注意：href 可能是相对路径，这里做简单包含匹配
                # 并在 worker_page 上操作
                try:
                    # 寻找 href 包含 link 或者是 link 结尾的元素
                    link_locator = worker_page.locator(f"a[href*='{link}']").first
                    
                    if link_locator.count() > 0 and link_locator.is_visible():
                        print("      [Action] 模拟点击进入 (新标签页)...")
                        with context.expect_page(timeout=10000) as new_page_info:
                            # 按住 Control 点击以在新标签页打开
                            worker_page.keyboard.down("Control")
                            link_locator.click()
                            worker_page.keyboard.up("Control")
                        cloud_page = new_page_info.value
                        cloud_page.wait_for_load_state("domcontentloaded")
                    else:
                        raise Exception("Element not found")
                except Exception as e:
                    # 降级：直接新建页面访问
                    print(f"      [Action] 元素未定位或点击失败，转为直接访问: {e}")
                    cloud_page = context.new_page()
                    response = cloud_page.goto(link, wait_until="domcontentloaded")
                    if response and response.status == 404:
                        handle_fatal_error(browser, link, "Cloud Disk Direct Access (网盘直连)")

                # 在 cloud_page 上执行后续操作
                time.sleep(1)
                
                # 检测 404 (如果是点击进来的，response 对象可能拿不到，检查标题或内容)
                if "404" in cloud_page.title() or "页面不存在" in cloud_page.inner_text("body"):
                     handle_fatal_error(browser, link, "Cloud Disk Clicked Page (网盘页面404特征检测)")

                # 尝试登录
                attempt_cloud_login(cloud_page, codes)

                # 确定文件夹
                folder_name = determine_local_folder(cloud_page, link)
                
                # 使用 Folder Mapping 机制分配路径 (确保重名不冲突)
                local_path = get_assigned_folder(link, folder_name, DOWNLOAD_ROOT)
                
                if not os.path.exists(local_path):
                    os.makedirs(local_path)
                created_dir_path = local_path
                
                print(f"    [Disk] 准备下载到: {local_path}")
                
                # 执行下载 (传入 cloud_page)
                mode, files = download_content(cloud_page, local_path)
                
                # 记录结果 (文件级别)
                record = {
                    "title": title,
                    "article_url": article_url,
                    "cloud_url": link,
                    "local_path": local_path,
                    "files_downloaded": files,
                    "status": mode,
                    "time": time.strftime("%Y-%m-%d %H:%M:%S")
                }
                save_record(record)
                
                # 清理空目录
                if not files and created_dir_path:
                    try:
                        if not os.listdir(created_dir_path):
                            os.rmdir(created_dir_path)
                            print(f"    [Cleanup] 空目录已删除")
                    except: pass
                    
            except Exception as e:
                print(f"    [Disk Error] {e}")
            finally:
                if cloud_page:
                    try: cloud_page.close()
                    except: pass

    except Exception as e:
        print(f"    [Post Error] 处理失败: {e}")
    finally:
        if worker_page:
            try: worker_page.close()
            except: pass


def run_spider():
    ensure_dirs()
    
    with sync_playwright() as p:
        # 启动浏览器
        browser = p.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO)
        # 必须开启 accept_downloads 用于下载
        context = browser.new_context(
            viewport={'width': 1280, 'height': 800},
            accept_downloads=True
        )
        page = context.new_page()
        
        print(f"--> 打开页面: {TARGET_URL}")
        response = page.goto(TARGET_URL, wait_until="domcontentloaded")
        if response and response.status == 404:
            handle_fatal_error(browser, TARGET_URL, "Main Feed Page (入口页)")
            
        page.wait_for_timeout(3000)
        
        # === 循环滚动与处理模式 ===
        print("--> 开始进入 [获取 -> 处理 -> 滚动] 循环模式...")
        
        last_item_count = 0
        no_change_counter = 0
        
        # 用于记录已处理过的 URL，防止重复
        processed_urls = set()
        
        for i in range(MAX_SCROLL_ATTEMPTS):
            # 1. 扫描当前页面上的所有文章链接
            elements = page.locator("a[href*='/article/']").all()
            
            # 识别本次扫描到的新内容
            new_items = []
            
            for el in elements:
                try:
                    href = el.get_attribute("href")
                    title = el.inner_text().replace('\n', ' ').strip()
                    if href:
                        full_url = urljoin(TARGET_URL, href)
                        if "/article/" in full_url:
                            # 关键：只添加尚未处理过的
                            if full_url not in processed_urls:
                                item = (full_url, title)
                                new_items.append(item)
                                processed_urls.add(full_url)
                except: continue
            
            current_total_count = len(processed_urls)
            print(f"    [Loop {i+1}] 累计发现文章: {current_total_count} | 本次新增: {len(new_items)}")
            
            # 2. 立即处理新发现的项目
            if new_items:
                # 若有新增，重置计数器
                no_change_counter = 0
                print(f"    -> 正在处理新增的 {len(new_items)} 篇文章...")
                
                for idx, (url, title) in enumerate(new_items):
                    if len(processed_urls) > MAX_PROCESS_LIMIT:
                        print("    -> 已达到最大处理限制，停止。")
                        browser.close()
                        return

                    process_single_article(context, browser, url, title)
            else:
                no_change_counter += 1
            
            # 3. 检查是否需要停止 (即使没有新内容，也可能因为还没滚动到底部)
            if no_change_counter >= NO_NEW_DATA_LIMIT:
                print("    -> 连续多次未发现新文章，停止滚动。")
                break
                
            # 4. 执行滚动加载更多
            print("    -> 滚动加载下一页...")
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            try:
                page.wait_for_timeout(SCROLL_PAUSE_TIME * 1000)
            except: pass

        print(f"--> 全部完成，结果已保存至: {OUTPUT_FILE}")
        browser.close()

if __name__ == "__main__":
    run_spider()
