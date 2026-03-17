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
COOKIES_FILE = os.path.join(PROJECT_ROOT, "miyoushe_cookies.json")  # 根目录cookie文件

# 爬取配置
MAX_SCROLL_ATTEMPTS = 1000  # 最大滚动次数 (增加以获取更多数据)
SCROLL_PAUSE_TIME = 2.0    # 每次滚动后等待时间(秒)
NO_NEW_DATA_LIMIT = 5      # 连续N次滚动没有新内容则停止
HEADLESS = True            # 无头模式运行
MAX_PROCESS_LIMIT = 5000   # 最大详情页处理数 (不限数量)
SLOW_MO = 100              # 下载时的操作延迟

# 统计信息
STATS = {
    "total_articles": 0,
    "articles_with_cloud": 0,
    "download_success": 0,
    "download_failed": 0,
    "page_anomaly": 0,
    "no_button": 0,
    "404_errors": 0,
    "skipped_existing": 0
}

# ================= 工具函数 =================
def ensure_dirs():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    if not os.path.exists(DOWNLOAD_ROOT):
        os.makedirs(DOWNLOAD_ROOT)

def save_cookies(context):
    """保存当前浏览器的Cookies到文件"""
    try:
        cookies = context.cookies()
        with open(COOKIES_FILE, "w", encoding="utf-8") as f:
            json.dump(cookies, f, indent=2, ensure_ascii=False)
        print(f"[Cookie] 登录状态已保存至: {COOKIES_FILE}")
        return True
    except Exception as e:
        print(f"[Cookie] 保存失败: {e}")
        return False

def load_cookies(context):
    """从文件加载Cookies到浏览器"""
    if not os.path.exists(COOKIES_FILE):
        return False
    try:
        with open(COOKIES_FILE, "r", encoding="utf-8") as f:
            cookies = json.load(f)
        context.add_cookies(cookies)
        print(f"[Cookie] 已加载登录状态 ({len(cookies)} 条)")
        return True
    except Exception as e:
        print(f"[Cookie] 加载失败: {e}")
        return False

def wait_for_manual_login(page, context):
    """等待用户手动登录并保存Cookies"""
    print("\n" + "="*60)
    print("检测到未登录状态，需要手动登录以绕过风控限制")
    print("="*60)
    print("请在浏览器中完成以下操作：")
    print("  1. 点击页面右上角的登录按钮")
    print("  2. 使用米游社APP扫码登录")
    print("  3. 登录成功后，回到此控制台")
    print("="*60)

    input("完成登录后，请按 [回车键] 继续...")

    # 保存登录后的Cookies
    if save_cookies(context):
        print("[Cookie] 登录状态已保存，下次运行将自动使用")
    else:
        print("[Cookie] 警告：登录状态保存失败，下次可能需要重新登录")

    print("="*60 + "\n")

def save_record(record):
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

def sanitize_filename(name, max_length=80):
    """清理文件名/文件夹名"""
    name = name.replace('\n', ' ').replace('\r', ' ')
    name = re.sub(r'[\\/:*?"<>|]', '_', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name[:max_length]

def handle_fatal_error(browser, url, context_info):
    """处理致命错误并记录日志（仅用于主页面入口的致命错误）"""
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

def log_404_skip(url, context_info, title=""):
    """记录404错误但不终止程序，跳过当前项继续处理"""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    log_content = (
        f"[{timestamp}] [404 SKIP] Page not found, skipping.\n"
        f"Context: {context_info}\n"
        f"Title: {title}\n"
        f"URL: {url}\n"
    )

    print(f"    [404 Skip] {context_info}")
    if title:
        print(f"    [404 Skip] 标题: {title}")
    print(f"    [404 Skip] URL: {url}")

    try:
        # 确保目录存在
        log_dir = os.path.dirname(ERROR_LOG_FILE)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(log_content + "-"*60 + "\n")
    except Exception as e:
        print(f"    [404 Skip] Warning: Failed to write log: {e}")

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
    """确定本地文件夹名（后备方案）"""
    folder_name = ""
    try:
        # 尝试从面包屑或其他位置获取
        elements = page.get_by_text(re.compile("当前路径|位置|Path")).all()
        for el in elements:
            if el.is_visible():
                txt = el.inner_text().strip()
                # 清理前缀（如"当前路径："、"位置："等）
                txt = re.sub(r'^(当前路径|位置|Path)\s*[:：]\s*', '', txt)
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

def check_page_anomaly(page):
    """检测页面异常状态（验证码、登录页、限流等）"""
    try:
        page_title = page.title()
        page_text = page.inner_text("body")[:1000]  # 取前1000字符检查
        page_url = page.url

        # 异常关键词检测
        anomaly_keywords = {
            "验证码": ["验证码", "captcha", "CAPTCHA", "人机验证", "滑动验证"],
            "登录": ["请登录", "登录后查看", "需要登录", "login required"],
            "限流": ["访问频繁", "请稍后再试", "too many requests", "rate limit", "系统繁忙"],
            "权限": ["无权限", "权限不足", "access denied", "forbidden"],
            "404": ["404", "页面不存在", "not found", "偏离了地球"]
        }

        detected_issues = []
        for issue_type, keywords in anomaly_keywords.items():
            if any(kw in page_title.lower() or kw in page_text.lower() for kw in keywords):
                detected_issues.append(issue_type)

        if detected_issues:
            print(f"      [异常检测] 发现页面异常: {', '.join(detected_issues)}")
            print(f"      [异常检测] 页面标题: {page_title}")
            print(f"      [异常检测] 页面URL: {page_url}")
            return True, detected_issues

        return False, []
    except Exception as e:
        print(f"      [异常检测] 检测失败: {e}")
        return False, []

def download_content(page, local_dir):
    """核心下载逻辑：只尝试ZIP打包下载（增强版：重试+日志+异常检测）"""
    downloaded_files = []
    mode = "failed"

    MAX_RETRIES = 3  # 最大重试次数
    WAIT_TIME = 5    # 初始等待时间（秒）

    for attempt in range(MAX_RETRIES):
        print(f"      [尝试 {attempt + 1}/{MAX_RETRIES}] 开始处理...")

        # 等待页面加载完成（逐步增加等待时间）
        wait_time = WAIT_TIME + (attempt * 2)  # 第一次5秒，第二次7秒，第三次9秒
        print(f"      [等待] 等待页面加载 {wait_time} 秒...")
        time.sleep(wait_time)

        # === 新增：页面异常检测 ===
        is_anomaly, issues = check_page_anomaly(page)
        if is_anomaly:
            print(f"      [异常] 检测到页面异常，停止下载: {issues}")
            mode = f"page_anomaly_{','.join(issues)}"

            # 保存截图用于调试
            try:
                screenshot_path = os.path.join(local_dir, f"error_screenshot_{int(time.time())}.png")
                page.screenshot(path=screenshot_path)
                print(f"      [调试] 已保存错误截图: {screenshot_path}")
            except:
                pass

            return mode, downloaded_files

        # === 详细日志：页面基本信息 ===
        try:
            page_title = page.title()
            page_url = page.url
            print(f"      [页面信息] 标题: {page_title[:50]}...")
            print(f"      [页面信息] URL: {page_url}")
        except Exception as e:
            print(f"      [页面信息] 获取失败: {e}")

        # 查找ZIP打包下载按钮
        print("      [ZIP] 正在查找ZIP打包下载按钮...")
        target_btn = None

        # 策略1: 查找所有按钮和链接，检查文本内容
        try:
            all_buttons = page.locator("button, a, div[role='button']").all()
            print(f"      [ZIP] 页面共有 {len(all_buttons)} 个可点击元素")

            zip_keywords = ["打包下载", "全部下载", "下载全部", "zip", "ZIP", "打包", "批量下载"]

            # === 新增：详细日志 - 列出所有可见按钮 ===
            visible_buttons = []
            for btn in all_buttons:
                try:
                    if not btn.is_visible():
                        continue

                    text = btn.inner_text().strip()
                    visible_buttons.append(text)

                    # 检查是否包含关键词
                    if any(keyword in text for keyword in zip_keywords):
                        print(f"      [ZIP] ✓ 找到候选按钮: '{text}'")
                        target_btn = btn
                        break
                except:
                    continue

            if not target_btn and visible_buttons:
                print(f"      [ZIP] 可见按钮列表 (前10个): {visible_buttons[:10]}")

        except Exception as e:
            print(f"      [ZIP] 查找按钮时出错: {e}")

        # 策略2: 如果策略1没找到，尝试通过CSS选择器
        if not target_btn:
            print("      [ZIP] 策略1未找到，尝试CSS选择器...")
            selectors = [
                "button:has-text('打包')",
                "button:has-text('下载')",
                "a:has-text('打包')",
                "a:has-text('ZIP')",
                "[class*='download'][class*='all']",
                "[class*='batch'][class*='download']"
            ]

            for selector in selectors:
                try:
                    elements = page.locator(selector).all()
                    for elem in elements:
                        if elem.is_visible():
                            text = elem.inner_text().strip()
                            print(f"      [ZIP] CSS选择器找到: '{text}'")
                            target_btn = elem
                            break
                    if target_btn:
                        break
                except:
                    continue

        # 如果找到按钮，尝试下载
        if target_btn:
            print(f"      [ZIP] 发现打包下载按钮，开始下载...")
            try:
                with page.expect_download(timeout=60000) as download_info:
                    target_btn.click()

                download = download_info.value
                safe_name = sanitize_filename(download.suggested_filename)
                save_path = os.path.join(local_dir, safe_name)
                download.save_as(save_path)
                print(f"      [ZIP] 下载完成: {safe_name}")

                # 解压处理
                if zipfile.is_zipfile(save_path):
                    try:
                        with zipfile.ZipFile(save_path, 'r') as zf:
                            zf.extractall(local_dir)
                            downloaded_files.extend(zf.namelist())
                        os.remove(save_path) # 删除原 ZIP
                        mode = "zip_extracted"
                        print(f"      [ZIP] 解压成功，共 {len(downloaded_files)} 个文件")
                    except Exception as e:
                        print(f"      [ZIP] 解压失败: {e}")
                        downloaded_files.append(safe_name)
                        mode = "zip_raw"
                else:
                    downloaded_files.append(safe_name)
                    mode = "zip_file"

                # 下载成功，退出重试循环
                return mode, downloaded_files

            except Exception as e:
                print(f"      [ZIP] 下载流程异常: {e}")
                if attempt < MAX_RETRIES - 1:
                    print(f"      [重试] 将在 3 秒后重试...")
                    time.sleep(3)
                    continue
                else:
                    mode = "download_failed"
        else:
            print(f"      [ZIP] 未找到ZIP打包下载按钮")
            if attempt < MAX_RETRIES - 1:
                print(f"      [重试] 将在 3 秒后重试...")
                time.sleep(3)
                continue
            else:
                mode = "no_zip_button"

                # === 最后一次尝试失败，保存截图 ===
                try:
                    screenshot_path = os.path.join(local_dir, f"no_button_screenshot_{int(time.time())}.png")
                    page.screenshot(path=screenshot_path)
                    print(f"      [调试] 已保存调试截图: {screenshot_path}")
                except:
                    pass

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
    # 1. 简单正则提取 URL (修复：排除中文字符和常见分隔符)
    # 使用更严格的匹配，只允许 ASCII 字符，遇到中文、空格等立即停止
    urls = re.findall(r"https?://[a-zA-Z0-9./?&_=%-]+", text)

    # 优先匹配 minas (米哈游专用)，并清理可能的尾部字符
    minas_links = []
    for u in urls:
        if "minas.mihoyo.com" in u:
            # 清理URL：移除尾部的非法字符（如 / 后面跟着的非预期内容）
            # minas链接格式通常是: https://minas.mihoyo.com/d/{hash}/
            match = re.match(r"(https://minas\.mihoyo\.com/d/[a-zA-Z0-9]+)/?", u)
            if match:
                minas_links.append(match.group(1) + "/")
            else:
                minas_links.append(u)
    
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

def check_if_already_downloaded(title):
    """检查该帖子是否已经下载过"""
    # 清理标题（与下载时的逻辑保持一致）
    clean_title = title.strip()
    clean_title = re.sub(r'\s+\d{4}-\d{2}-\d{2}$', '', clean_title)
    clean_title = re.sub(r'\s+\d{2}-\d{2}$', '', clean_title)
    clean_title = re.sub(r'\s+\d+小时前$', '', clean_title)
    clean_title = re.sub(r'\s+\d+天前$', '', clean_title)

    folder_name = sanitize_filename(clean_title)
    expected_path = os.path.join(DOWNLOAD_ROOT, folder_name)

    # 检查文件夹是否存在且非空
    if os.path.exists(expected_path) and os.path.isdir(expected_path):
        contents = os.listdir(expected_path)
        # 过滤掉截图文件，只检查实际下载的内容
        actual_files = [f for f in contents if not f.startswith('error_screenshot_') and not f.startswith('no_button_screenshot_')]
        if actual_files:
            return True, expected_path

    # 检查是否有带序号的文件夹（如 folder_name_01, folder_name_02）
    if os.path.exists(DOWNLOAD_ROOT):
        for item in os.listdir(DOWNLOAD_ROOT):
            item_path = os.path.join(DOWNLOAD_ROOT, item)
            if os.path.isdir(item_path):
                # 检查是否匹配 folder_name 或 folder_name_XX 格式
                if item == folder_name or re.match(rf'^{re.escape(folder_name)}_\d{{2}}$', item):
                    contents = os.listdir(item_path)
                    actual_files = [f for f in contents if not f.startswith('error_screenshot_') and not f.startswith('no_button_screenshot_')]
                    if actual_files:
                        return True, item_path

    return False, None

def process_single_article(context, browser, article_url, title):
    """(Refactored) 处理单个详情页，包含提取云盘链接和下载"""
    global STATS
    STATS["total_articles"] += 1

    # === 新增：检查是否已经下载过 ===
    already_downloaded, existing_path = check_if_already_downloaded(title)
    if already_downloaded:
        STATS["skipped_existing"] += 1
        print(f"  [Skip] 已存在: {title[:30]}... -> {existing_path}")
        return

    worker_page = None
    try:
        worker_page = context.new_page()
        print(f"  [Processing] 分析: {title[:30]}...")

        # 访问详情页
        response = worker_page.goto(article_url, wait_until="domcontentloaded", timeout=45000)
        if response and response.status == 404:
            STATS["404_errors"] += 1
            log_404_skip(article_url, "文章详情页返回404", title)
            return

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
                STATS["404_errors"] += 1
                log_404_skip(article_url, "文章详情页Soft 404检测", title)
                return
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

        STATS["articles_with_cloud"] += 1
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
                        log_404_skip(link, "云盘直连返回404", title)
                        if cloud_page:
                            try: cloud_page.close()
                            except: pass
                        continue

                # 在 cloud_page 上执行后续操作
                print(f"      [云盘] 等待页面稳定...")
                time.sleep(2)

                # === 增强：云盘页面异常检测 ===
                try:
                    cloud_page.wait_for_load_state("domcontentloaded", timeout=10000)
                except Exception as e:
                    print(f"      [云盘] 页面加载超时: {e}")

                # 检测 404 (如果是点击进来的，response 对象可能拿不到，检查标题或内容)
                if "404" in cloud_page.title() or "页面不存在" in cloud_page.inner_text("body"):
                    log_404_skip(link, "云盘页面404特征检测", title)
                    if cloud_page:
                        try: cloud_page.close()
                        except: pass
                    continue

                # === 新增：云盘页面状态日志 ===
                try:
                    cloud_title = cloud_page.title()
                    cloud_url = cloud_page.url
                    print(f"      [云盘] 页面标题: {cloud_title[:50]}...")
                    print(f"      [云盘] 当前URL: {cloud_url}")
                except:
                    pass

                # 尝试登录
                print(f"      [云盘] 检查是否需要密码...")
                login_result = attempt_cloud_login(cloud_page, codes)
                if login_result:
                    print(f"      [云盘] 密码验证成功: {login_result}")
                else:
                    print(f"      [云盘] 无需密码或密码验证失败")

                # 确定文件夹名称：只使用帖子标题
                # 清理标题中的非法字符和日期后缀
                clean_title = title.strip()
                # 移除常见的日期格式（如 "2025-03-05", "03-05", "11小时前" 等）
                clean_title = re.sub(r'\s+\d{4}-\d{2}-\d{2}$', '', clean_title)
                clean_title = re.sub(r'\s+\d{2}-\d{2}$', '', clean_title)
                clean_title = re.sub(r'\s+\d+小时前$', '', clean_title)
                clean_title = re.sub(r'\s+\d+天前$', '', clean_title)

                folder_name = sanitize_filename(clean_title)

                # 直接使用帖子标题作为文件夹名，不使用映射机制
                local_path = os.path.join(DOWNLOAD_ROOT, folder_name)

                # 如果文件夹已存在且非空，添加序号避免覆盖
                if os.path.exists(local_path) and os.listdir(local_path):
                    counter = 1
                    while True:
                        new_path = f"{local_path}_{counter:02d}"
                        if not os.path.exists(new_path) or not os.listdir(new_path):
                            local_path = new_path
                            break
                        counter += 1
                
                if not os.path.exists(local_path):
                    os.makedirs(local_path)
                created_dir_path = local_path
                
                print(f"    [Disk] 准备下载到: {local_path}")
                
                # 执行下载 (传入 cloud_page)
                mode, files = download_content(cloud_page, local_path)

                # === 新增：统计下载结果 ===
                if mode in ["zip_extracted", "zip_raw", "zip_file"]:
                    STATS["download_success"] += 1
                elif "page_anomaly" in mode:
                    STATS["page_anomaly"] += 1
                    STATS["download_failed"] += 1
                elif mode == "no_zip_button":
                    STATS["no_button"] += 1
                    STATS["download_failed"] += 1
                else:
                    STATS["download_failed"] += 1

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

        # 尝试加载已保存的Cookies
        cookies_loaded = load_cookies(context)
        if cookies_loaded:
            print("[Auth] 使用已保存的登录状态")
        else:
            print("[Auth] 未找到登录状态，将以游客身份访问")
            print("[Auth] 如遇到404错误，程序会提示您登录")

        page = context.new_page()

        print(f"--> 打开页面: {TARGET_URL}")
        response = page.goto(TARGET_URL, wait_until="domcontentloaded")
        if response and response.status == 404:
            handle_fatal_error(browser, TARGET_URL, "Main Feed Page (入口页)")

        page.wait_for_timeout(3000)

        # 检查是否需要登录（如果没有加载Cookies或Cookies已过期）
        if not cookies_loaded:
            # 检查页面是否正常加载（简单判断：是否有文章链接）
            article_count = page.locator("a[href*='/article/']").count()
            if article_count == 0:
                print("[Auth] 警告：页面可能需要登录才能访问")
                wait_for_manual_login(page, context)
                # 重新加载页面
                page.reload(wait_until="domcontentloaded")
                page.wait_for_timeout(3000)

        # === 循环滚动与处理模式 ===
        print("--> 开始进入 [获取 -> 处理 -> 滚动] 循环模式...")

        last_item_count = 0
        no_change_counter = 0

        # 用于记录已处理过的 URL，防止重复
        processed_urls = set()

        for i in range(MAX_SCROLL_ATTEMPTS):
            # === 新增：每100个文章输出一次状态报告 ===
            if len(processed_urls) > 0 and len(processed_urls) % 100 == 0:
                print(f"\n{'='*60}")
                print(f"[状态报告] 已处理 {len(processed_urls)} 篇文章")
                print(f"[状态报告] 跳过已下载: {STATS['skipped_existing']}")
                print(f"[状态报告] 当前循环次数: {i+1}/{MAX_SCROLL_ATTEMPTS}")
                print(f"[状态报告] 连续无新内容次数: {no_change_counter}/{NO_NEW_DATA_LIMIT}")

                # 检查Cookie是否仍然有效
                try:
                    current_cookies = context.cookies()
                    print(f"[状态报告] 当前Cookie数量: {len(current_cookies)}")
                except:
                    print(f"[状态报告] Cookie检查失败")

                print(f"{'='*60}\n")

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

        # === 新增：输出统计报告 ===
        print(f"\n{'='*60}")
        print(f"统计报告")
        print(f"{'='*60}")
        print(f"总文章数: {STATS['total_articles']}")
        print(f"跳过已下载: {STATS['skipped_existing']}")
        print(f"包含云盘链接的文章: {STATS['articles_with_cloud']}")
        print(f"下载成功: {STATS['download_success']}")
        print(f"下载失败: {STATS['download_failed']}")
        print(f"  - 页面异常: {STATS['page_anomaly']}")
        print(f"  - 找不到按钮: {STATS['no_button']}")
        print(f"404错误: {STATS['404_errors']}")
        print(f"{'='*60}\n")

        browser.close()

if __name__ == "__main__":
    run_spider()
