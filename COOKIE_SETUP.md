# Cookie配置指南

## 为什么需要Cookie？

米游社爬虫需要登录状态才能正常访问内容，避免触发风控限制。本项目提供两种方式配置Cookie。

## 方式一：从浏览器导出（推荐）

### 步骤

1. **登录米游社**
   - 访问 https://www.miyoushe.com
   - 使用米游社APP扫码登录

2. **导出Cookie**
   - 按 `F12` 打开浏览器开发者工具
   - 切换到 **Console（控制台）** 标签
   - 如果是Chrome，先输入 `允许粘贴` 并回车
   - 复制 `export_cookies.js` 中的代码并粘贴到控制台
   - 按回车执行，Cookie会自动复制到剪贴板

3. **保存Cookie**
   - 在项目根目录创建 `cookie.txt` 文件
   - 粘贴刚才复制的JSON内容并保存

4. **转换Cookie**
   ```bash
   python convert_cookies.py
   ```
   成功后会显示：
   ```
   成功读取 12 条cookie
   已保存到: miyoushe_cookies.json

   转换完成! 1 个文件已更新
   现在可以直接运行爬虫,将自动使用已保存的登录状态
   ```

5. **运行爬虫**
   ```bash
   # 单线程版本
   python zzz_scroll_spider.py

   # 多线程版本
   python zzz_scroll_spider_mt_new.py
   ```

## 方式二：首次运行时手动登录

如果不想手动导出Cookie，可以直接运行爬虫：

```bash
python zzz_scroll_spider.py
```

程序会：
1. 自动打开浏览器
2. 检测到未登录时提示你扫码登录
3. 登录成功后自动保存Cookie
4. 下次运行自动使用保存的登录状态

## Cookie文件说明

- `cookie.txt` - 从浏览器导出的原始cookie（本地保留，不上传）
- `miyoushe_cookies.json` - 转换后的playwright格式cookie（项目根目录）

## 安全提示

⚠️ **重要**:
- Cookie包含你的登录凭证，请勿分享给他人
- 所有cookie文件已加入 `.gitignore`，不会被提交到Git仓库
- 如果cookie过期，重新执行上述步骤即可

## 常见问题

**Q: 转换工具报错"找不到有效的cookie JSON数据"？**

A: 确保 `cookie.txt` 包含完整的JSON数组，格式如下：
```json
[
  {
    "name": "_MHYUUID",
    "value": "...",
    "domain": ".miyoushe.com",
    ...
  },
  ...
]
```

**Q: 爬虫运行时提示404错误？**

A: Cookie可能已过期，重新导出并转换即可。

**Q: 可以在多台电脑上使用同一个cookie吗？**

A: 可以，但建议每台电脑独立登录，避免账号异常。
