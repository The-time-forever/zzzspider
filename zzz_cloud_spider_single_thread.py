import re
import json
import os
import time
import zipfile
import shutil
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# ================= 配置区域 =================
# 是否无头模式 (True=不显示浏览器, False=显示)
HEADLESS = False
# 操作减速 (毫秒)
SLOW_MO = 100
# 目录页入口
CATALOG_URL = "https://zzz.mihoyo.com/news?utm_source=oolandingpage"
# 基础数据存目录
DATA_DIR = "d:/Users/22542/Desktop/zzzspider/data"
# 下载保存目录
DOWNLOAD_ROOT = "d:/Users/22542/Desktop/zzzspider/downloads"
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

# ==============================================================================
# Part 1: 详情页处理器 (复用并封装之前的逻辑)
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

def process_news_detail(page, news_url, output_root):
    """处理单个新闻详情页"""
    result = {
        "news_url": news_url,
        "cloud_links_found": [],
        "processed_disks": [],
        "status": "success",
        "error_msg": ""
    }
    
    try:
        print(f"  > [Detail] 打开新闻页: {news_url}")
        page.goto(news_url, wait_until="domcontentloaded", timeout=45000)
        try:
            page.wait_for_load_state("networkidle", timeout=5000)
        except: pass
        
        text = page.inner_text("body")
        cloud_links, pwds = extract_from_text(text)
        result["cloud_links_found"] = cloud_links
        
        if not cloud_links:
            print("    -> 无云盘链接")
            return result

        print(f"    -> 找到 {len(cloud_links)} 个云盘链接")
        
        for link in cloud_links:
            disk_res = {
                "url": link, 
                "pwd": None, 
                "mode": "pending", 
                "local_folder": None,
                "files": []
            }
            
            # 记录是否创建了文件夹，以便回滚
            created_dir_path = None

            try:
                page.goto(link, wait_until="domcontentloaded", timeout=45000)
                time.sleep(1)
                
                used_pwd = attempt_cloud_login(page, pwds)
                disk_res["pwd"] = used_pwd
                
                try:
                    page.wait_for_load_state("networkidle", timeout=5000)
                except: pass
                
                # 确定文件夹名 (使用映射表管理)
                folder_name = determine_local_folder(page, link)
                local_path = get_assigned_folder(link, folder_name, output_root)
                
                if not os.path.exists(local_path):
                    os.makedirs(local_path)
                created_dir_path = local_path
                
                disk_res["local_folder"] = local_path
                print(f"    -> [Disk] 下载到: {local_path}")
                
                mode, files = download_content(page, local_path)
                disk_res["mode"] = mode
                disk_res["files"] = files

                # === 下载结果为空时的清理 (仅当文件夹为空时) ===
                if not files and created_dir_path:
                    try:
                        if not os.listdir(created_dir_path):
                            # 注意：如果删除了空文件夹，映射表里的记录并不会删除，
                            # 这意味着下次重跑还是会分配同一个路径。这是符合预期的(重试)。
                            os.rmdir(created_dir_path)
                            print(f"    -> [Cleanup] 空目录已删除: {created_dir_path}")
                    except Exception as clean_err:
                        print(f"    -> [Cleanup Warn] {clean_err}")
                # ============================================
                
            except Exception as e:
                print(f"    -> [Disk Error] {e}")
                disk_res["error"] = str(e)
            
            result["processed_disks"].append(disk_res)
            
    except Exception as e:
        result["status"] = "error"
        result["error_msg"] = str(e)
        print(f"  > [Detail Error] {e}")
        
    return result

# ==============================================================================
# Part 2: 目录页采集器
# ==============================================================================

def collect_news_urls(page, catalog_url):
    """
    采集所有新闻链接 (支持分页 + URL 归一化)
    返回: set(urls) 列表
    """
    collected_urls = set()
    tabs = ["最新"]
    
    print(f"--> [Collector] 访问目录页: {catalog_url}")
    try:
        page.goto(catalog_url, wait_until="networkidle", timeout=60000)
    except:
        page.goto(catalog_url, wait_until="load", timeout=60000)

    # 内部辅助函数：提取当前页面可见的有效新闻链接
    def extract_current_page_links():
        s = set()
        try:
            # 扫描页面所有 link
            links = page.locator("a[href]").all()
            for link in links:
                href = link.get_attribute("href")
                if not href: continue
                
                # 1. 转绝对路径
                full_url = urljoin("https://zzz.mihoyo.com", href)
                # 2. 解析 path，去除 query
                parsed = urlparse(full_url)
                path = parsed.path
                
                # 3. 匹配 /news/<id>
                if re.match(r"^/news/\d+$", path):
                    # 4. 归一化 (scheme + netloc + path)
                    clean_url = f"{parsed.scheme}://{parsed.netloc}{path}"
                    s.add(clean_url)
        except Exception as e:
            print(f"      [Scan Warn] 链接提取部分失败: {e}")
        return s

    for tab_name in tabs:
        print(f"\n--> [Collector] 切换 Tab: {tab_name}")
        # 1. 切换 Tab
        try:
            # 尝试点击 Tab
            tab_locator = page.get_by_text(tab_name, exact=True)
            if tab_locator.count() > 0 and tab_locator.first.is_visible():
                tab_locator.first.click()
                time.sleep(1.5) # 等待列表刷新
            else:
                print(f"    [Warn] Tab '{tab_name}' 未找到，跳过。")
                continue
            
            # 强制回到第一页 (防止之前操作残留)
            try:
                page1_btn = page.locator("a.mihoyo-pager-rich__button").filter(has_text="1")
                if page1_btn.count() > 0 and page1_btn.first.is_visible():
                     # 检查当前是否已经是第一页
                    current_chk = page.locator("a.mihoyo-pager-rich__button.mihoyo-pager-rich__current")
                    if current_chk.count() > 0 and current_chk.first.inner_text().strip() != "1":
                        print("    [Reset] 点击回到第 1 页...")
                        page1_btn.first.click()
                        time.sleep(1.5)
            except Exception as e:
                print(f"    [Reset Warn] 回到第一页尝试失败: {e}")

        except Exception as e:
            print(f"    [Tab Error] {tab_name}: {e}")
            continue

        # 2. 分页循环
        page_index = 1
        max_page_per_tab = 200 # 防止死循环
        empty_page_count = 0 
        
        while page_index <= max_page_per_tab:
            # A. 提取链接 (当前页)
            current_page_urls = extract_current_page_links()
            
            # B. 统计去重
            new_added = 0
            for u in current_page_urls:
                if u not in collected_urls:
                    collected_urls.add(u)
                    new_added += 1
            
            # 获取当前页码显示
            try:
                curr_el = page.locator("a.mihoyo-pager-rich__button.mihoyo-pager-rich__current").first
                curr_txt = curr_el.inner_text().strip() if curr_el.count() > 0 else str(page_index)
            except:
                curr_txt = str(page_index)

            print(f"    [Tab:{tab_name}] [Page {curr_txt}] 本页识别: {len(current_page_urls)} 条 | 新增: {new_added} | 总计: {len(collected_urls)}")

            # C. 终止条件检查
            if len(current_page_urls) == 0:
                empty_page_count += 1
                if empty_page_count >= 3:
                     print("    -> 连续多次未获取到链接，判定为 Tab 结束。")
                     break
            else:
                empty_page_count = 0

            # D. 执行翻页 (Strategy A: current -> next sibling)
            try:
                # 重新定位 current 元素确保不过期
                current_active = page.locator("a.mihoyo-pager-rich__button.mihoyo-pager-rich__current").first
                if not current_active.is_visible():
                    print("    [Pagination] 未找到当前页高亮元素，可能只有一页或结构变化。")
                    break
                
                old_page_num = current_active.inner_text().strip()
                
                # 寻找下一个兄弟页码按钮
                # 使用 xpath 找紧邻的下一个 class 包含 mihoyo-pager-rich__button 的元素
                next_page_btn = current_active.locator("xpath=following-sibling::a[contains(@class,'mihoyo-pager-rich__button')][1]")
                
                if next_page_btn.count() > 0 and next_page_btn.is_visible():
                    next_num = next_page_btn.inner_text().strip()
                    # print(f"    -> 准备翻页: {old_page_num} -> {next_num}")
                    
                    # 为了更稳的等待：记录翻页前第一个新闻标题用于对比
                    first_news_title = ""
                    try:
                        first_news = page.locator("a[href*='/news/']").first
                        if first_news.is_visible():
                            first_news_title = first_news.inner_text()
                    except: pass

                    # 点击下一页
                    next_page_btn.click()
                    
                    # 等待翻页成功的信号 (current 页码变化)
                    page_changed = False
                    for _ in range(20): # max 10s
                        time.sleep(0.5)
                        try:
                            check_curr = page.locator("a.mihoyo-pager-rich__button.mihoyo-pager-rich__current").first
                            if check_curr.is_visible() and check_curr.inner_text().strip() != old_page_num:
                                page_changed = True
                                break
                        except: pass
                    
                    if not page_changed:
                         print(f"    [Pagination] 翻页超时 (当前页码 {old_page_num} 未变)，可能已是末页或卡住。")
                         break
                    else:
                        page_index += 1
                        # 额外等待内容渲染完成 (网络空闲)
                        try:
                            # 如果能对比标题最好
                            if first_news_title:
                                # 简单等待直到标题不再是旧的，或者超时
                                pass 
                            page.wait_for_load_state("networkidle", timeout=2000)
                        except: pass

                else:
                    print("    [Pagination] 没有下一个页码按钮了，本 Tab 结束。")
                    break

            except Exception as e:
                print(f"    [Pagination] 翻页异常: {e}")
                break
                
    # 最终去重排序
    final_list = sorted(list(collected_urls), reverse=True)
    return final_list

# ==============================================================================
# Part 3: 主控逻辑 (断点续跑 + 调度)
# ==============================================================================

def main():
    print("=== 全站采集脚本启动 ===")
    
    # 1. 准备断点记录
    processed_file = os.path.join(DATA_DIR, "processed_news.json")
    results_file = os.path.join(DATA_DIR, "results.json")
    news_urls_file = os.path.join(DATA_DIR, "news_urls.json")
    
    processed_set = set()
    if os.path.exists(processed_file):
        try:
            with open(processed_file, "r", encoding="utf-8") as f:
                processed_set = set(json.load(f))
        except: pass
    
    # 2. 启动浏览器采集目录
    # 注意：为了避免长时间运行的 context 内存问题，采集完目录后可以重启一个 context，
    # 或者直接复用。这里复用。
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=HEADLESS, 
            slow_mo=SLOW_MO,
            args=["--start-maximized"]
        )
        context = browser.new_context(
            accept_downloads=True,
            viewport={'width': 1920, 'height': 1080}
        )
        
        # 2.1 采集目录 (除非我们想跳过采集直接用本地缓存)
        # 这里每次都采集一下，防止有新内容
        page = context.new_page()
        try:
            all_news_urls = collect_news_urls(page, CATALOG_URL)
        except Exception as e:
            print(f"采集出错: {e}")
            all_news_urls = []
        page.close()
        
        # 保存采集到的 URL 列表
        with open(news_urls_file, "w", encoding="utf-8") as f:
            json.dump(all_news_urls, f, indent=2)
            
        print(f"\n--> 采集完成，共 {len(all_news_urls)} 个链接")
        
        # 2.2 过滤任务
        tasks = []
        for url in all_news_urls:
            if url not in processed_set:
                tasks.append(url)
        
        print(f"--> 需要处理的任务: {len(tasks)} (已跳过 {len(processed_set)} 个)")
        
        if MAX_NEWS_LIMIT:
            print(f"    (测试模式) 仅处理前 {MAX_NEWS_LIMIT} 个")
            tasks = tasks[:MAX_NEWS_LIMIT]

        # 2.3 执行处理
        full_results = []
        # 按需加载已有结果
        if os.path.exists(results_file):
            try:
                with open(results_file, "r", encoding="utf-8") as f:
                    full_results = json.load(f)
            except: pass

        worker_page = context.new_page()
        
        for i, url in enumerate(tasks):
            print(f"\n[{i+1}/{len(tasks)}] 开始任务: {url}")
            
            # 调用详情页处理器
            result_data = process_news_detail(worker_page, url, DOWNLOAD_ROOT)
            
            # 更新已处理集合
            processed_set.add(url)
            full_results.append(result_data)
            
            # 实时落盘 (防止崩溃丢失)
            with open(processed_file, "w", encoding="utf-8") as f:
                json.dump(list(processed_set), f, indent=2)
            
            with open(results_file, "w", encoding="utf-8") as f:
                json.dump(full_results, f, indent=2, ensure_ascii=False)
                
            # 简单限频
            time.sleep(1)

        print("\n=== 全部任务结束 ===")
        browser.close()

if __name__ == "__main__":
    main()
