import re
import json
import time
import os
import shutil
import zipfile
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright

# ================= 配置区域 =================
# 目标页面：米游社-绝区零-官方资讯
TARGET_URL = "https://www.miyoushe.com/zzz/home/58?type=3"
# 数据保存路径 (独立于之前的文件夹)
DATA_DIR = "d:/Users/22542/Desktop/zzzspider/data_scroll_ver"
DOWNLOAD_ROOT = "d:/Users/22542/Desktop/zzzspider/downloads_scroll_ver"
OUTPUT_FILE = os.path.join(DATA_DIR, "scroll_spider_results.jsonl")

# 爬取配置
MAX_SCROLL_ATTEMPTS = 300  # 最大滚动次数 (增加以获取更多数据)
SCROLL_PAUSE_TIME = 2.0    # 每次滚动后等待时间(秒)
NO_NEW_DATA_LIMIT = 5      # 连续N次滚动没有新内容则停止
HEADLESS = False           # 显示浏览器以便观察滚动效果
MAX_PROCESS_LIMIT = 5000   # 最大详情页处理数
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
             pass
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
        page.goto(TARGET_URL, wait_until="domcontentloaded")
        page.wait_for_timeout(3000)
        
        # === 阶段 1: 无限滚动采集链接 ===
        print("--> 开始滚动采集列表...")
        
        last_item_count = 0
        no_change_counter = 0
        collected_links = set() # (url, title)
        
        for i in range(MAX_SCROLL_ATTEMPTS):
            # 1. 提取当前页面的所有文章链接
            # 米游社通常是 a[href*='/article/']
            elements = page.locator("a[href*='/article/']").all()
            
            current_batch = set()
            for el in elements:
                try:
                    href = el.get_attribute("href")
                    title = el.inner_text().replace('\n', ' ').strip()
                    if href:
                        full_url = urljoin(TARGET_URL, href)
                        if "/article/" in full_url:
                            current_batch.add((full_url, title))
                except: continue
            
            # 更新总集合
            for item in current_batch:
                collected_links.add(item)
            
            current_count = len(collected_links)
            print(f"    [Scroll {i+1}] 当前捕获文章数: {current_count}")
            
            # 2. 检查是否有新内容
            if current_count > last_item_count:
                last_item_count = current_count
                no_change_counter = 0
            else:
                no_change_counter += 1
            
            if no_change_counter >= NO_NEW_DATA_LIMIT:
                print("    -> 连续多次未发现新文章，停止滚动。")
                break
                
            # 3. 执行滚动
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            try:
                page.wait_for_timeout(SCROLL_PAUSE_TIME * 1000)
            except: pass

        print(f"--> 列表采集完成，共 {len(collected_links)} 篇文章。")
        
        # === 阶段 2: 逐个处理详情页并下载 ===
        print("--> 开始进入详情页提取并下载...")
        
        worker_page = context.new_page()
        
        for idx, (article_url, title) in enumerate(collected_links):
            if idx >= MAX_PROCESS_LIMIT:
                print(f"    (限制模式) 已达最大处理数 {MAX_PROCESS_LIMIT}，停止。")
                break
                
            print(f"  [{idx+1}/{len(collected_links)}] 分析: {title[:30]}...")
            
            try:
                # 访问详情页
                worker_page.goto(article_url, wait_until="domcontentloaded", timeout=45000)
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
                     print("    -> 无云盘链接")
                     continue

                print(f"    -> 发现云盘链接: {len(all_cloud_links)} 个")
                
                # 开始下载流程
                for link in all_cloud_links:
                    print(f"    --> 处理链接: {link}")
                    
                    # 记录是否创建文件夹，用于空目录清理
                    created_dir_path = None
                    
                    try:
                        # 访问网盘页
                        worker_page.goto(link, wait_until="domcontentloaded", timeout=45000)
                        time.sleep(1)
                        
                        # 尝试登录
                        attempt_cloud_login(worker_page, codes)

                        # 确定文件夹
                        folder_name = determine_local_folder(worker_page, link)
                        
                        # 使用 Folder Mapping 机制分配路径 (确保重名不冲突)
                        local_path = get_assigned_folder(link, folder_name, DOWNLOAD_ROOT)
                        
                        if not os.path.exists(local_path):
                            os.makedirs(local_path)
                        created_dir_path = local_path
                        
                        print(f"    [Disk] 准备下载到: {local_path}")
                        
                        # 执行下载
                        mode, files = download_content(worker_page, local_path)
                        
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

            except Exception as e:
                print(f"    [Post Error] 处理失败: {e}")
            
            time.sleep(1)

        print(f"--> 全部完成，结果已保存至: {OUTPUT_FILE}")
        browser.close()

if __name__ == "__main__":
    run_spider()
