import json
from playwright.sync_api import sync_playwright

URL = "https://www.miyoushe.com/zzz/home/58?type=3"

def looks_like_list_api(url: str) -> bool:
    u = url.lower()
    # 这只是启发式，你运行后看输出再把条件改精确
    return ("api" in u or "feed" in u or "post" in u or "timeline" in u) and ("miyoushe" in u or "mihoyo" in u)

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()

    def on_response(resp):
        try:
            ct = (resp.headers.get("content-type") or "").lower()
            if "application/json" not in ct:
                return
            if not looks_like_list_api(resp.url):
                return
            data = resp.json()
            print("\n=== JSON RESPONSE ===")
            print("URL:", resp.url)
            print("Keys:", list(data.keys()) if isinstance(data, dict) else type(data))
            print("Preview:", json.dumps(data, ensure_ascii=False)[:800])
        except Exception:
            pass

    page.on("response", on_response)
    page.goto(URL, wait_until="networkidle")
    page.wait_for_timeout(3000)

    browser.close()
