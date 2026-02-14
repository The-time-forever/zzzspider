# ZZZ Official News Spider (绝区零官网爬虫)

这是一个基于 Python 和 Microsoft Playwright 的自动化爬虫工具，用于采集 [绝区零官网](https://zzz.mihoyo.com/news) 的最新新闻资讯，并自动提取和下载其中包含的官方云盘资源（壁纸、素材等）。

## 主要功能

*   **全量采集**: 自动遍历官网"最新"栏目的所有新闻分页。
*   **智能提取**: 自动识别新闻正文中的云盘链接（minas.mihoyo.com）及提取码。
*   **自动登录**: 模拟用户行为，自动输入提取码并进入云盘页面。
*   **资源下载**: 
    *   优先尝试点击 "打包下载" (ZIP)。
    *   若无打包按钮，自动降级为逐个下载图片/视频文件。
    *   自动解压 ZIP 文件并清理压缩包。
*   **断点续传**:
    *   记录已处理的新闻 URL，重启脚本时自动跳过。
    *   智能检测本地文件是否存在，避免重复通过网络下载。
*   **目录映射**: 内置 `folder_map.json` 机制，解决不同新闻对应相同默认文件夹名（如“壁纸分享”）导致的冲突问题，确保每个链接的内容下载到专属的文件夹。

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

### 方案 A：标准分页采集 (推荐)
基于翻页按钮逻辑，适用于精确控制、断点续传。

1.  **运行命令**
    ```bash
    python zzz_cloud_spider_single_thread.py
    ```
2.  **数据存放**
    *   数据目录: `data/`
    *   下载目录: `downloads/`

### 方案 B：无限滚动采集
模拟用户滚动页面到底部的行为，适用于简单的列表抓取或分页器不规则时。

1.  **运行命令**
    ```bash
    python zzz_scroll_spider.py
    ```
2.  **数据存放 (独立)**
    *   数据目录: `data_scroll_ver/`
    *   下载目录: `downloads_scroll_ver/`

## 配置调整
可在脚本头部调整变量：
*   `HEADLESS = False`: 设置为 `True` 可隐藏浏览器界面后台运行。
*   `MAX_NEWS_LIMIT`: 限制采集数量（测试用）。

## 目录结构

*   `zzz_cloud_spider_single_thread.py`: 方案A 主脚本。
*   `zzz_scroll_spider.py`: 方案B 主脚本。
*   `data/` & `data_scroll_ver/`: 存放运行时数据 (JSON, Map)。
*   `downloads/` & `downloads_scroll_ver/`: 下载的资源文件存放处。

##以此项目供学习交流使用。
