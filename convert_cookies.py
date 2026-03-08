#!/usr/bin/env python3
"""
Cookie转换工具
从cookie.txt读取浏览器导出的cookie JSON,转换为playwright格式并保存
"""
import json
import os
import sys

def convert_cookies():
    """将cookie.txt中的cookie转换为playwright格式"""

    # 文件路径
    cookie_txt = "cookie.txt"
    output_dirs = ["."]  # 根目录
    output_filename = "miyoushe_cookies.json"

    # 检查cookie.txt是否存在
    if not os.path.exists(cookie_txt):
        print(f"错误: 找不到 {cookie_txt} 文件")
        print("请先按照README说明从浏览器导出cookie")
        return False

    # 读取cookie.txt
    try:
        with open(cookie_txt, 'r', encoding='utf-8') as f:
            content = f.read()

        # 查找JSON数组部分(从第一个[到最后一个])
        # 使用更智能的方式：找到第一个完整的JSON数组
        start_idx = -1
        for i, char in enumerate(content):
            if char == '[':
                # 尝试从这里解析JSON
                try:
                    end_idx = content.find('\n]', i)
                    if end_idx == -1:
                        end_idx = content.rfind(']')
                    else:
                        end_idx += 2  # 包含 \n]

                    test_json = content[i:end_idx]
                    cookies = json.loads(test_json)

                    # 验证是否是cookie数组
                    if isinstance(cookies, list) and len(cookies) > 0:
                        if all(isinstance(c, dict) and 'name' in c and 'value' in c for c in cookies):
                            start_idx = i
                            print(f"成功读取 {len(cookies)} 条cookie")
                            break
                except:
                    continue

        if start_idx == -1:
            print("错误: cookie.txt中没有找到有效的cookie JSON数据")
            print("请确保cookie.txt包含从浏览器控制台导出的cookie数组")
            return False

    except json.JSONDecodeError as e:
        print(f"错误: cookie.txt格式不正确 - {e}")
        return False
    except Exception as e:
        print(f"错误: 读取cookie.txt失败 - {e}")
        return False

    # 转换为playwright格式
    playwright_cookies = []
    for cookie in cookies:
        # playwright需要的字段
        pw_cookie = {
            "name": cookie.get("name", ""),
            "value": cookie.get("value", ""),
            "domain": cookie.get("domain", ".miyoushe.com"),
            "path": cookie.get("path", "/"),
            "secure": cookie.get("secure", True),
            "httpOnly": cookie.get("httpOnly", False),
            "sameSite": cookie.get("sameSite", "Lax"),
        }
        # expires: -1表示会话cookie,playwright用None或不传
        expires = cookie.get("expires", -1)
        if expires and expires != -1:
            pw_cookie["expires"] = expires

        playwright_cookies.append(pw_cookie)

    # 保存到各输出目录
    success_count = 0
    for output_dir in output_dirs:
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, output_filename)
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(playwright_cookies, f, indent=2, ensure_ascii=False)
            print(f"已保存到: {output_path}")
            success_count += 1
        except Exception as e:
            print(f"保存失败 {output_path}: {e}")

    if success_count > 0:
        print(f"\n转换完成! {success_count} 个目录已更新")
        print("现在可以直接运行爬虫,将自动使用已保存的登录状态")
        return True
    return False


if __name__ == "__main__":
    convert_cookies()

