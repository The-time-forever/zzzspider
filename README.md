# ZZZ Spider Collection (绝区零爬虫工具集)

这是一个基于 Python 和 Microsoft Playwright 的自动化爬虫工具集，用于采集绝区零相关的新闻资讯，并自动提取和下载其中包含的官方云盘资源（壁纸、素材等）。

本项目包含多个爬虫脚本，分别针对不同的数据源：
- **绝区零官方网站** (https://zzz.mihoyo.com/news)
- **米游社社区** (https://www.miyoushe.com/zzz)

## 主要功能

*   **全量采集**: 自动遍历新闻分页，获取所有资讯。
*   **智能提取**: 自动识别新闻正文中的云盘链接（minas.mihoyo.com）及提取码。
*   **自动登录**:
    *   支持Cookie保存和自动加载
    *   首次运行可手动扫码登录，后续自动使用保存的登录状态
    *   绕过米游社风控限制
*   **资源下载**:
    *   只尝试ZIP打包下载（最高效）
    *   自动解压 ZIP 文件并清理压缩包
*   **智能命名**: 使用帖子标题作为文件夹名称，清晰易识别
*   **404处理**: 遇到404页面自动跳过并记录日志，不中断程序
*   **断点续传**: 记录已处理的URL，重启时自动跳过
*   **多线程支持**: 异步并发处理，速度提升3倍

## 环境要求

*   Windows / macOS / Linux
*   Python 3.8+
*   Playwright

## 安装步骤

1.  **创建/激活虚拟环境 (可选但推荐)**
    ```bash
    python -m venv venv
    # Windows
    .\venv\Scripts\activate
    # Mac/Linux
    source venv/bin/activate
    ```

2.  **安装依赖库**
    ```bash
    pip install playwright
    ```

3.  **安装浏览器驱动**
    ```bash
    playwright install chromium
    ```

## 使用说明

### 方案 A：绝区零官方网站爬虫
针对 [绝区零官方网站](https://zzz.mihoyo.com/news)，基于翻页按钮逻辑，适用于精确控制、断点续传。

1.  **运行命令**
    ```bash
    python zzz_cloud_spider_single_thread.py
    ```
2.  **数据存放**
    *   数据目录: `data/`
    *   下载目录: `downloads/`

### 方案 B：米游社网站爬虫（推荐）
针对 [米游社绝区零板块](https://www.miyoushe.com/zzz)，提供单线程和多线程两种版本。

#### B1. 单线程版本（稳定）
适合网络不稳定或需要观察运行过程的场景。

1.  **运行命令**
    ```bash
    python zzz_scroll_spider.py
    ```
2.  **数据存放**
    *   数据目录: `ZZZ_Miyoushe_Cloud_Download/data/`
    *   下载目录: `ZZZ_Miyoushe_Cloud_Download/downloads/`

#### B2. 多线程版本（高效）⚡
使用异步并发，同时处理3篇文章，速度提升约3倍。

1.  **运行命令**
    ```bash
    python zzz_scroll_spider_mt_new.py
    ```
2.  **数据存放**
    *   数据目录: `ZZZ_Miyoushe_Cloud_Download_MT/data/`
    *   下载目录: `ZZZ_Miyoushe_Cloud_Download_MT/downloads/`
3.  **配置调整**
    *   `CONCURRENCY_LIMIT = 3`: 并发数量（可根据网络情况调整）

#### B3. API 调用版本
直接调用米游社 API 接口，效率更高但可能受限于 API 限制。

1.  **运行命令**
    ```bash
    python zzz_api_spider.py
    ```
2.  **数据存放**
    *   数据目录: `data/`
    *   下载目录: `downloads/`

## 配置调整
可在脚本头部调整变量：
*   `HEADLESS = False`: 设置为 `True` 可隐藏浏览器界面后台运行
*   `MAX_PROCESS_LIMIT = 5000`: 限制采集数量
*   `CONCURRENCY_LIMIT = 3`: 多线程版本的并发数量（仅多线程版本）
*   `COOKIES_FILE`: Cookie保存位置（自动管理）

## 首次使用说明

1. **首次运行米游社爬虫**：
   - 程序会自动打开浏览器
   - 如果检测到需要登录，会提示你手动扫码登录
   - 登录成功后按回车，程序会自动保存登录状态
   - 下次运行会自动使用保存的Cookie，无需重复登录

2. **Cookie文件位置**：
   - 单线程版本：`ZZZ_Miyoushe_Cloud_Download/data/miyoushe_cookies.json`
   - 多线程版本：`ZZZ_Miyoushe_Cloud_Download_MT/data/miyoushe_cookies.json`

## 目录结构

### 主要脚本文件
*   `zzz_cloud_spider_single_thread.py`: 绝区零官方网站爬虫（单线程版本）
*   `zzz_cloud_spider_multi_thread.py`: 绝区零官方网站爬虫（多线程版本）
*   `zzz_scroll_spider.py`: 米游社网站爬虫（单线程，最新版本）⭐
*   `zzz_scroll_spider_mt_new.py`: 米游社网站爬虫（多线程，最新版本）⚡
*   `zzz_scroll_spider_mt.py`: 米游社网站爬虫（旧多线程版本）
*   `zzz_api_spider.py`: 米游社网站爬虫（API 版本）
*   `mystry.py`: 辅助工具脚本（用于探测 API 接口）
*   `export_cookies.js`: Cookie导出辅助脚本

### 数据目录
*   `data/`: 官方网站爬虫的运行时数据 (JSON, Map)
*   `downloads/`: 官方网站爬虫的资源文件存放处
*   `ZZZ_Miyoushe_Cloud_Download/`: 米游社爬虫（单线程）的数据和下载目录
*   `ZZZ_Miyoushe_Cloud_Download_MT/`: 米游社爬虫（多线程）的数据和下载目录

##以此项目供学习交流使用。

## 项目说明

本项目包含多个爬虫脚本，主要区别如下：

| 脚本名称 | 目标网站 | 实现方式 | 并发 | 特点 |
|---------|---------|---------|------|------|
| `zzz_cloud_spider_single_thread.py` | 绝区零官网 | 翻页按钮 | 单线程 | 稳定可靠 |
| `zzz_cloud_spider_multi_thread.py` | 绝区零官网 | 翻页按钮 | 多线程 | 速度更快 |
| `zzz_scroll_spider.py` | 米游社 | 页面滚动 | 单线程 | 最新功能，稳定 ⭐ |
| `zzz_scroll_spider_mt_new.py` | 米游社 | 页面滚动 | 3并发 | 最新功能，高效 ⚡ |
| `zzz_api_spider.py` | 米游社 | API 调用 | 单线程 | 效率高，但可能受限 |

**推荐使用**：
- 采集官网资讯：`zzz_cloud_spider_single_thread.py`
- 采集米游社资讯（稳定）：`zzz_scroll_spider.py`
- 采集米游社资讯（快速）：`zzz_scroll_spider_mt_new.py`

**最新版本特性**（`zzz_scroll_spider.py` 和 `zzz_scroll_spider_mt_new.py`）：
- ✅ Cookie自动管理，绕过风控
- ✅ 404自动跳过，不中断程序
- ✅ 使用帖子标题命名文件夹
- ✅ 只尝试ZIP下载（最高效）
- ✅ URL提取优化，避免格式错误
