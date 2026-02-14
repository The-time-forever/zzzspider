import os
import re
import json
import time
import random
import urllib.request
import urllib.parse
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# ================= 配置区域 =================
# 米游社 API 配置
MIYOUSHE_API_LIST = "https://bbs-api-static.miyoushe.com/painter/wapi/getNewsList?client_type=4&gids=8&last_id={}&page_size=20&type=3"
MIYOUSHE_API_DETAIL = "https://bbs-api-static.miyoushe.com/post/wapi/getPostFull?gids=8&post_id={}&read=1"

# 数据保存路径
DATA_DIR = "d:/Users/22542/Desktop/zzzspider/data"
DOWNLOAD_ROOT = "d:/Users/22542/Desktop/zzzspider/downloads"
CLOUD_LINKS_FILE = os.path.join(DATA_DIR, "cloud_links.jsonl")

# 爬取配置
MAX_PAGES = 5  # 每次运行爬取列表页数
HEADLESS_MODE = False # 调试时设为 False，实际部署可 True (但也建议False以便人工接入)

# ================= 工具函数 =================
def ensure_dirs():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    if not os.path.exists(DOWNLOAD_ROOT):
        os.makedirs(DOWNLOAD_ROOT)

def load_processed_posts():
    ids = set()
    if os.path.exists(CLOUD_LINKS_FILE):
        with open(CLOUD_LINKS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    data = json.loads(line)
                    ids.add(data.get("post_id"))
                except: pass
    return ids

def save_cloud_record(record):
    with open(CLOUD_LINKS_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

# ================= Part A: 发现阶段 (Discovery) =================

class MiyousheScanner:
    def __init__(self):
        self.processed_posts = load_processed_posts()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://www.miyoushe.com/"
        }

    def fetch_json(self, url):
        try:
            req = urllib.request.Request(url, headers=self.headers)
            with urllib.request.urlopen(req) as resp:
                if resp.status == 200:
                    return json.loads(resp.read().decode('utf-8'))
        except Exception as e:
            print(f"[API Error] {url}: {e}")
        return None

    def scan_news_list(self):
        last_id = ""
        found_items = []
        
        print(f"--> 开始扫描米游社资讯列表 (前 {MAX_PAGES} 页)...")
        
        for page_num in range(MAX_PAGES):
            target_url = MIYOUSHE_API_LIST.format(last_id)
            print(f"    Scanning Page {page_num+1}...")
            
            data = self.fetch_json(target_url)
            if not data or data.get("retcode") != 0:
                print("    API返回异常或结束")
                break
                
            posts = data.get("data", {}).get("list", [])
            if not posts:
                print("    本页无数据")
                break
            
            for item in posts:
                post_info = item.get("post", {})
                post_id = post_info.get("post_id")
                subject = post_info.get("subject")
                
                # 更新 last_id 用于翻页
                last_id = post_id
                
                # 如果这个帖子已经处理过（且没有增量更新需求），理论上可以跳过
                # 但为了防止之前漏抓，暂不在此处强跳过，除非量非常大
                # 这里先只打印
                # print(f"      Check: {post_id} - {subject}")
                
                details = self.process_post_detail(post_id, subject)
                if details:
                    found_items.extend(details)
                    
            time.sleep(random.uniform(1.0, 2.0)) # 礼貌限频

        return found_items

    def process_post_detail(self, post_id, title):
        # 结果暂存
        records = []
        
        # 1. 优先尝试 API 获取详情
        api_url = MIYOUSHE_API_DETAIL.format(post_id)
        data = self.fetch_json(api_url)
        content = ""
        
        if data and data.get("retcode") == 0:
            post_data = data.get("data", {}).get("post", {})
            content = post_data.get("content", "") # HTML content
            # 也有 structured_content，但 content 是 html 包含链接更直观
            
            # 如果 API 没有内容，可能需要 Playwright (作为 Fallback，暂略，遵循 '优先 JSON' 指示)
        
        if not content:
            return []

        # 2. 提取云盘链接
        # 识别常见网盘域名
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
        
        text_for_search = re.sub(r'<[^>]+>', ' ', content) # 简单去标点方便搜密码
        
        # 查找所有链接
        # HTML 中的 href
        hrefs = re.findall(r'href=[\'"]?(https?://[^\'" >]+)', content)
        # 纯文本中的链接
        text_links = re.findall(r'https?://[a-zA-Z0-9./?&_=-]+', text_for_search)
        
        all_candidates = set(hrefs + text_links)
        
        valid_links = []
        for link in all_candidates:
            for domain_pat in pan_domains:
                if re.search(domain_pat, link):
                    valid_links.append(link)
                    break
        
        if not valid_links:
            return []

        print(f"    [Post {post_id}] 发现 {len(valid_links)} 个潜在云盘链接: {title}")
        
        # 3. 提取密码 (简单上下文搜索)
        # 在整个文本中搜可能的密码，简单起见，不针对每个链接做极其复杂的距离计算
        # 而是提取所有 "码: XXXX" 形式，然后尝试匹配
        codes = []
        code_patterns = [
            r"(?:密码|提取码|访问码|口令)\s*[:：]\s*([A-Za-z0-9]{4})",
            r"(?:code)\s*[:：]\s*([A-Za-z0-9]{4})"
        ]
        for pat in code_patterns:
            found = re.findall(pat, text_for_search)
            codes.extend(found)
        
        # 去重
        codes = list(set(codes))
        default_code = codes[0] if codes else None
        
        for v_link in valid_links:
            # 判重：如果已经记录过 (post_id + cloud_url)，跳过
            # 在这里做简单记录构造
            rec = {
                "post_id": post_id,
                "title": title,
                "article_url": f"https://www.miyoushe.com/zzz/article/{post_id}",
                "cloud_url": v_link,
                "code": default_code, # 暂时只关联找到的第一个码，多码情况需要更复杂逻辑
                "found_context": "API/Regex",
                "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": "pending" # pending, downloading, done, failed
            }
            records.append(rec)
            
            # 立即保存（防止 Crash）
            # 检查是否重复 (简单检查内存中的 processed_posts 是不够的，因为一个 post 可能有多个 link)
            # 这里简单追加，execute 阶段再去重处理
            save_cloud_record(rec)
            
        return records

# ================= Part B: 执行阶段 (Execution) =================

class CloudDownloader:
    def __init__(self):
        self.playwright = None
        self.browser = None
        self.context = None
    
    def start(self):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=HEADLESS_MODE, slow_mo=1000)
        self.context = self.browser.new_context(accept_downloads=True)
    
    def stop(self):
        if self.context: self.context.close()
        if self.browser: self.browser.close()
        if self.playwright: self.playwright.stop()

    def process_pending_links(self):
        # 读取所有记录
        all_records = []
        if not os.path.exists(CLOUD_LINKS_FILE):
            return
            
        with open(CLOUD_LINKS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    all_records.append(json.loads(line))
                except: pass
        
        # 筛选 pending
        pending = [r for r in all_records if r.get("status") == "pending"]
        print(f"--> 待处理云盘任务: {len(pending)} 个")
        
        if not pending:
            return

        self.start()
        try:
            for rec in pending:
                print(f"  > 处理: {rec['cloud_url']} (Code: {rec['code']})")
                new_status, note = self.dispatch_adapter(rec)
                
                # 更新状态 (简单粗暴：全部重写文件太慢，这里仅打印，实际项目需数据库)
                # 为了演示，我们只追加状态变更日志或不做持久化修改
                print(f"    Result: {new_status} - {note}")
                
        finally:
            self.stop()

    def dispatch_adapter(self, record):
        url = record['cloud_url']
        code = record['code']
        page = self.context.new_page()
        res = ("skipped", "unknown_provider")
        
        try:
            if "pan.baidu.com" in url:
                res = self.adapter_baidu(page, url, code, record['post_id'])
            # 可扩展其他 adapter
            else:
                print("    [Warn] 暂不支持该网盘，跳过")
                res = ("skipped", "unsupported")
        except Exception as e:
            print(f"    [Error] 未捕获异常: {e}")
            res = ("failed", str(e))
        finally:
            page.close()
        return res

    def adapter_baidu(self, page, url, code, post_id):
        # 1. 打开页面
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
        except:
            return "failed", "timeout_load"

        # 2. 检查提取码
        # 百度网盘特征：如果有提取码，会有 input#accessCode
        try:
            if page.locator("input#accessCode").is_visible(timeout=5000):
                print("    [Baidu] 需要提取码")
                if not code:
                    return "manual_needed", "missing_code"
                
                page.fill("input#accessCode", code)
                page.click("a#getfileBtn, a.g-button[title='提取文件']")
                # 等待跳转
                try:
                    page.wait_for_navigation(timeout=5000)
                except: time.sleep(2)
                
                # 检查是否密码错误
                if page.locator("input#accessCode").is_visible():
                    return "failed", "wrong_code"
        except: pass

        # 3. 检查是否需要登录
        # 百度很多时候看文件可以直接看，但下载大文件需要登录
        # 寻找下载按钮
        try:
            page.wait_for_selector(".g-button, .btn-download", timeout=10000)
        except:
            pass # 可能直接是文件列表

        # 4. 下载逻辑
        # 场景A: 单文件页 -> 直接点下载
        # 场景B: 列表页 -> 全选 -> 下载
        
        # 尝试检测 "登录" 弹窗/强制
        if page.locator("p.login-title, .login-main").is_visible():
             return "aborted", "login_required"

        # 定位下载按钮
        download_btn = page.locator("a[title='下载'], a.g-button:has-text('下载')").first
        
        if download_btn.is_visible():
            print("    [Baidu] 发现下载按钮，尝试点击...")
            
            # 监听下载
            try:
                with page.expect_download(timeout=10000) as download_info:
                    download_btn.click()
                    
                download = download_info.value
                # 保存路径
                save_dir = os.path.join(DOWNLOAD_ROOT, str(post_id))
                if not os.path.exists(save_dir):
                    os.makedirs(save_dir)
                
                final_path = os.path.join(save_dir, download.suggested_filename)
                download.save_as(final_path)
                print(f"    [Success] 下载完成: {final_path}")
                return "success", final_path
                
            except PlaywrightTimeoutError:
                # 可能是点了下载弹出了登录框，或者需要客户端
                if page.locator(".dialog-login, #login-container").is_visible():
                     return "aborted", "login_triggered_on_click"
                return "failed", "download_timeout"
            except Exception as e:
                return "failed", str(e)
        
        # 如果是列表页，可能需要先选文件
        # 简化：只处理第一层
        check_all = page.locator("div.z-grid-item.checkAll-container").first
        if check_all.is_visible():
            # 全选
            check_all.click()
            time.sleep(1)
            # 再找下载按钮
            download_btn = page.locator("a[title='下载'], a.g-button:has-text('下载')").first
            if download_btn.is_visible():
                # 同上下载逻辑... (略，复用上方代码块结构)
                pass
        
        return "manual_check", "no_download_action_found"


# ================= Main =================
def main():
    ensure_dirs()
    
    # 1. 扫描
    scanner = MiyousheScanner()
    scanner.scan_news_list()
    
    # 2. 下载
    downloader = CloudDownloader()
    downloader.process_pending_links()

if __name__ == "__main__":
    main()
