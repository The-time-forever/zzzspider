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
# 数据保存路径 (相对路径 - ZZZ_Miyoushe_Cloud_Download_MT)
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
BASE_OUTPUT_DIR = os.path.join(PROJECT_ROOT, "ZZZ_Miyoushe_Cloud_Download_MT")
DATA_DIR = os.path.join(BASE_OUTPUT_DIR, "data")
DOWNLOAD_ROOT = os.path.join(BASE_OUTPUT_DIR, "downloads")
OUTPUT_FILE = os.path.join(DATA_DIR, "scroll_spider_results.jsonl")
ERROR_LOG_FILE = os.path.join(BASE_OUTPUT_DIR, "spider_error.log")
COOKIES_FILE = os.path.join(DATA_DIR, "miyoushe_cookies.json")  # Cookie保存文件

# 爬取配置
MAX_SCROLL_ATTEMPTS = 1000  # 最大滚动次数 (增加以获取更多数据)
SCROLL_PAUSE_TIME = 2.0    # 每次滚动后等待时间(秒)
NO_NEW_DATA_LIMIT = 5      # 连续N次滚动没有新内容则停止
HEADLESS = True            # 无头模式运行
MAX_PROCESS_LIMIT = 5000   # 最大详情页处理数 (不限数量)
SLOW_MO = 100              # 下载时的操作延迟
CONCURRENCY_LIMIT = 3      # 并发处理数量

# ================= 全局锁 =================
file_write_lock = asyncio.Lock()
log_write_lock = asyncio.Lock()

# ================= 工具函数 =================
def ensure_dirs():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    if not os.path.exists(DOWNLOAD_ROOT):
        os.makedirs(DOWNLOAD_ROOT)

async def save_cookies(context):
    """保存当前浏览器的Cookies到文件"""
    try:
        cookies = await context.cookies()
        with open(COOKIES_FILE, "w", encoding="utf-8") as f:
            json.dump(cookies, f, indent=2, ensure_ascii=False)
        print(f"[Cookie] 登录状态已保存至: {COOKIES_FILE}")
        return True
    except Exception as e:
        print(f"[Cookie] 保存失败: {e}")
        return False

async def load_cookies(context):
    """从文件加载Cookies到浏览器"""
    if not os.path.exists(COOKIES_FILE):
        return False
    try:
        with open(COOKIES_FILE, "r", encoding="utf-8") as f:
            cookies = json.load(f)
        await context.add_cookies(cookies)
        print(f"[Cookie] 已加载登录状态 ({len(cookies)} 条)")
        return True
    except Exception as e:
        print(f"[Cookie] 加载失败: {e}")
        return False

async def wait_for_manual_login(page, context):
    """等待用户手动登录并保存Cookies"""
    print("\\n" + "="*60)
    print("检测到未登录状态，需要手动登录以绕过风控限制")
    print("="*60)
    print("请在浏览器中完成以下操作：")
    print("  1. 点击页面右上角的登录按钮")
    print("  2. 使用米游社APP扫码登录")
    print("  3. 登录成功后，回到此控制台")
    print("="*60)

    # 使用 asyncio 的方式等待用户输入
    await asyncio.get_event_loop().run_in_executor(None, input, "完成登录后，请按 [回车键] 继续...")

    # 保存登录后的Cookies
    if await save_cookies(context):
        print("[Cookie] 登录状态已保存，下次运行将自动使用")
    else:
        print("[Cookie] 警告：登录状态保存失败，下次可能需要重新登录")

    print("="*60 + "\\n")

async def save_record(record):
    async with file_write_lock:
        with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\\n")

def sanitize_filename(name, max_length=80):
    """清理文件名/文件夹名"""
    name = re.sub(r'[\\\\/:*?"<>|]', '_', name)
    name = re.sub(r'\\s+', ' ', name).strip()
    return name[:max_length]

async def handle_fatal_error(browser, url, context_info):
    """处理致命错误并记录日志（仅用于主页面入口的致命错误）"""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    log_content = (
        f"[{timestamp}] [FATAL ERROR] 404 Not Found detected.\\n"
        f"Context: {context_info}\\n"
        f"URL: {url}\\n"
    )

    print(f"\\n{'!'*60}")
    print(f"!!! 致命错误: 检测到 404 页面或无法访问的内容 !!!")
    print(f"!!! 发生位置: {context_info}")
    print(f"!!! 故障链接: {url}")
    print(f"!!! 详细日志已保存至: {ERROR_LOG_FILE}")
    print(f"!!! 程序已紧急停止以防止错误扩散。")
    print(f"{'!'*60}\\n")

    try:
        # 确保目录存在
        log_dir = os.path.dirname(ERROR_LOG_FILE)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        async with log_write_lock:
            with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(log_content + "-"*60 + "\\n")
    except Exception as e:
        print(f"Warning: Failed to write error log: {e}")

    if browser:
        try:
            await browser.close()
        except: pass
    sys.exit(1)

async def log_404_skip(url, context_info, title=""):
    """记录404错误但不终止程序，跳过当前项继续处理"""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    log_content = (
        f"[{timestamp}] [404 SKIP] Page not found, skipping.\\n"
        f"Context: {context_info}\\n"
        f"Title: {title}\\n"
        f"URL: {url}\\n"
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

        async with log_write_lock:
            with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(log_content + "-"*60 + "\\n")
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
    used_paths = set(p.lower().replace('\\\\', '/') for p in mapping.values())

    counter = 1
    # 冲突检测：路径物理存在 OR 路径已被其他 URL 预占
    while True:
        check_path_norm = final_path.lower().replace('\\\\', '/')
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
    """确定本地文件夹名（后备方案）"""
    folder_name = ""
    try:
        # 尝试从面包屑或其他位置获取
        elements = await page.get_by_text(re.compile("当前路径|位置|Path")).all()
        for el in elements:
            if await el.is_visible():
                txt = (await el.inner_text()).strip()
                # 清理前缀（如"当前路径："、"位置："等）
                txt = re.sub(r'^(当前路径|位置|Path)\\s*[:：]\\s*', '', txt)
                if 5 < len(txt) < 100:
                    folder_name = txt
                    break
    except: pass

    if not folder_name:
        title = await page.title()
        title = title.strip()
        if title and "mihoyo" not in title.lower():
            folder_name = title
        else:
            folder_name = url.rstrip('/').split('/')[-1] or f"disk_{int(time.time())}"

    return sanitize_filename(folder_name)

async def download_content(page, local_dir):
    """核心下载逻辑：只尝试ZIP打包下载"""
    downloaded_files = []
    mode = "failed"

    # 等待页面加载完成
    await asyncio.sleep(2)

    # 查找ZIP打包下载按钮
    print("      [ZIP] 正在查找ZIP打包下载按钮...")
    target_btn = None

    # 策略1: 查找所有按钮和链接，检查文本内容
    try:
        all_buttons = await page.locator("button, a, div[role='button']").all()
        print(f"      [ZIP] 页面共有 {len(all_buttons)} 个可点击元素")

        zip_keywords = ["打包下载", "全部下载", "下载全部", "zip", "ZIP", "打包", "批量下载"]

        for btn in all_buttons:
            try:
                if not await btn.is_visible():
                    continue

                text = (await btn.inner_text()).strip()

                # 检查是否包含关键词
                if any(keyword in text for keyword in zip_keywords):
                    print(f"      [ZIP] 找到候选按钮: '{text}'")
                    target_btn = btn
                    break
            except:
                continue

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
                elements = await page.locator(selector).all()
                for elem in elements:
                    if await elem.is_visible():
                        text = (await elem.inner_text()).strip()
                        print(f"      [ZIP] CSS选择器找到: '{text}'")
                        target_btn = elem
                        break
                if target_btn:
                    break
            except:
                continue

    if target_btn:
        print(f"      [ZIP] 发现打包下载按钮，开始下载...")
        try:
            async with page.expect_download(timeout=60000) as download_info:
                await target_btn.click()

            download = await download_info.value
            safe_name = sanitize_filename(download.suggested_filename)
            save_path = os.path.join(local_dir, safe_name)
            await download.save_as(save_path)
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
        except Exception as e:
            print(f"      [ZIP] 下载流程异常: {e}")
            mode = "failed"
    else:
        print("      [ZIP] 未找到ZIP打包下载按钮")
        mode = "no_zip_button"

    return mode, downloaded_files


# ================= 核心逻辑 =================

def extract_cloud_info_from_text(text):
    """从文本中提取云盘链接和密码"""
    # 常见网盘域名
    pan_domains = [
        r"pan\\.baidu\\.com/s/[\\w-]+",
        r"yun\\.baidu\\.com/s/[\\w-]+",
        r"aliyundrive\\.com/s/[\\w-]+",
        r"alipan\\.com/s/[\\w-]+",
        r"cloud\\.189\\.cn/t/[\\w-]+",
        r"lanzou\\w?\\.com/[\\w]+",
        r"quark\\.cn/s/[\\w-]+",
        r"123pan\\.com/s/[\\w-]+"
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
            match = re.match(r"(https://minas\\.mihoyo\\.com/d/[a-zA-Z0-9]+)/?", u)
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
        r"(?:密码|提取码|访问码|口令)\\s*[:：]\\s*([A-Za-z0-9]{4,})",
        r"(?:code)\\s*[:：]\\s*([A-Za-z0-9]{4,})"
    ]
    for pat in code_patterns:
        found = re.findall(pat, text)
        codes.extend(found)

    # 合并去重
    return list(set(minas_links + other_links)), list(set(codes))

async def process_single_article(context, browser, article_url, title):
    """(Refactored) 处理单个详情页，包含提取云盘链接和下载"""
    worker_page = None
    try:
        worker_page = await context.new_page()
        print(f"  [Processing] 分析: {title[:30]}...")

        # 访问详情页
        response = await worker_page.goto(article_url, wait_until="domcontentloaded", timeout=45000)
        if response and response.status == 404:
            await log_404_skip(article_url, "文章详情页返回404", title)
            return

        # === 新增: Soft 404 检测 (针对 HTTP 200 但内容错误的页面) ===
        try:
            page_title = await worker_page.title()
            page_text_start = (await worker_page.inner_text("body"))[:500] # 只取前500字符快速检查

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
                    if await worker_page.locator("text=404").count() > 0:
                        is_soft_404 = True
                    # 或检测包含 "偏离了地球" 的元素
                    elif await worker_page.get_by_text("偏离了地球").count() > 0:
                        is_soft_404 = True
                except: pass

            if is_soft_404:
                await log_404_skip(article_url, "文章详情页Soft 404检测", title)
                return
        except Exception:
            pass # 页面可能还没渲染完，或者是非致命错误，继续往下走

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

                    if await link_locator.count() > 0 and await link_locator.is_visible():
                        print("      [Action] 模拟点击进入 (新标签页)...")
                        async with context.expect_page(timeout=10000) as new_page_info:
                            # 按住 Control 点击以在新标签页打开
                            await worker_page.keyboard.down("Control")
                            await link_locator.click()
                            await worker_page.keyboard.up("Control")
                        cloud_page = await new_page_info.value
                        await cloud_page.wait_for_load_state("domcontentloaded")
                    else:
                        raise Exception("Element not found")
                except Exception as e:
                    # 降级：直接新建页面访问
                    print(f"      [Action] 元素未定位或点击失败，转为直接访问: {e}")
                    cloud_page = await context.new_page()
                    response = await cloud_page.goto(link, wait_until="domcontentloaded")
                    if response and response.status == 404:
                        await log_404_skip(link, "云盘直连返回404", title)
                        if cloud_page:
                            try: await cloud_page.close()
                            except: pass
                        continue

                # 在 cloud_page 上执行后续操作
                await asyncio.sleep(1)

                # 检测 404 (如果是点击进来的，response 对象可能拿不到，检查标题或内容)
                cloud_title = await cloud_page.title()
                cloud_body = await cloud_page.inner_text("body")
                if "404" in cloud_title or "页面不存在" in cloud_body:
                    await log_404_skip(link, "云盘页面404特征检测", title)
                    if cloud_page:
                        try: await cloud_page.close()
                        except: pass
                    continue

                # 尝试登录
                await attempt_cloud_login(cloud_page, codes)

                # 确定文件夹名称：只使用帖子标题
                # 清理标题中的非法字符和日期后缀
                clean_title = title.strip()
                # 移除常见的日期格式（如 "2025-03-05", "03-05", "11小时前" 等）
                clean_title = re.sub(r'\\s+\\d{4}-\\d{2}-\\d{2}$', '', clean_title)
                clean_title = re.sub(r'\\s+\\d{2}-\\d{2}$', '', clean_title)
                clean_title = re.sub(r'\\s+\\d+小时前$', '', clean_title)
                clean_title = re.sub(r'\\s+\\d+天前$', '', clean_title)

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
                mode, files = await download_content(cloud_page, local_path)

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
                await save_record(record)

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
                    try: await cloud_page.close()
                    except: pass

    except Exception as e:
        print(f"    [Post Error] 处理失败: {e}")
    finally:
        if worker_page:
            try: await worker_page.close()
            except: pass


async def run_spider():
    ensure_dirs()

    async with async_playwright() as p:
        # 启动浏览器
        browser = await p.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO)
        # 必须开启 accept_downloads 用于下载
        context = await browser.new_context(
            viewport={'width': 1280, 'height': 800},
            accept_downloads=True
        )

        # 尝试加载已保存的Cookies
        cookies_loaded = await load_cookies(context)
        if cookies_loaded:
            print("[Auth] 使用已保存的登录状态")
        else:
            print("[Auth] 未找到登录状态，将以游客身份访问")
            print("[Auth] 如遇到404错误，程序会提示您登录")

        page = await context.new_page()

        print(f"--> 打开页面: {TARGET_URL}")
        response = await page.goto(TARGET_URL, wait_until="domcontentloaded")
        if response and response.status == 404:
            await handle_fatal_error(browser, TARGET_URL, "Main Feed Page (入口页)")

        await page.wait_for_timeout(3000)

        # 检查是否需要登录（如果没有加载Cookies或Cookies已过期）
        if not cookies_loaded:
            # 检查页面是否正常加载（简单判断：是否有文章链接）
            article_count = await page.locator("a[href*='/article/']").count()
            if article_count == 0:
                print("[Auth] 警告：页面可能需要登录才能访问")
                await wait_for_manual_login(page, context)
                # 重新加载页面
                await page.reload(wait_until="domcontentloaded")
                await page.wait_for_timeout(3000)

        # === 循环滚动与处理模式 ===
        print("--> 开始进入 [获取 -> 处理 -> 滚动] 循环模式...")

        last_item_count = 0
        no_change_counter = 0

        # 用于记录已处理过的 URL，防止重复
        processed_urls = set()

        # 创建信号量控制并发
        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

        async def process_with_semaphore(url, title):
            async with semaphore:
                await process_single_article(context, browser, url, title)

        for i in range(MAX_SCROLL_ATTEMPTS):
            # 1. 扫描当前页面上的所有文章链接
            elements = await page.locator("a[href*='/article/']").all()

            # 识别本次扫描到的新内容
            new_items = []

            for el in elements:
                try:
                    href = await el.get_attribute("href")
                    title_text = await el.inner_text()
                    title = title_text.replace('\\n', ' ').strip()
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

            # 2. 立即处理新发现的项目（使用并发）
            if new_items:
                # 若有新增，重置计数器
                no_change_counter = 0
                print(f"    -> 正在并发处理新增的 {len(new_items)} 篇文章 (并发数: {CONCURRENCY_LIMIT})...")

                # 检查是否超过最大处理限制
                if len(processed_urls) > MAX_PROCESS_LIMIT:
                    print("    -> 已达到最大处理限制，停止。")
                    await browser.close()
                    return

                # 使用 asyncio.gather 并发处理所有新文章
                tasks = [process_with_semaphore(url, title) for url, title in new_items]
                await asyncio.gather(*tasks)
            else:
                no_change_counter += 1

            # 3. 检查是否需要停止 (即使没有新内容，也可能因为还没滚动到底部)
            if no_change_counter >= NO_NEW_DATA_LIMIT:
                print("    -> 连续多次未发现新文章，停止滚动。")
                break

            # 4. 执行滚动加载更多
            print("    -> 滚动加载下一页...")
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            try:
                await page.wait_for_timeout(SCROLL_PAUSE_TIME * 1000)
            except: pass

        print(f"--> 全部完成，结果已保存至: {OUTPUT_FILE}")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(run_spider())
