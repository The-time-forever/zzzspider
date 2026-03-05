// 在米游社网站的浏览器控制台中运行此脚本
// 步骤：
// 1. 打开 https://www.miyoushe.com/zzz/home/58?type=3 并登录
// 2. 按 F12 打开开发者工具
// 3. 切换到 Console (控制台) 标签
// 4. 复制粘贴下面的代码并按回车
// 5. 复制输出的 JSON 内容

(function() {
    const cookies = document.cookie.split(';').map(cookie => {
        const [name, value] = cookie.trim().split('=');
        return {
            name: name,
            value: value,
            domain: '.miyoushe.com',
            path: '/',
            expires: -1,
            httpOnly: false,
            secure: true,
            sameSite: 'Lax'
        };
    });

    console.log('='.repeat(60));
    console.log('请复制下面的 JSON 内容：');
    console.log('='.repeat(60));
    console.log(JSON.stringify(cookies, null, 2));
    console.log('='.repeat(60));
    console.log(`共导出 ${cookies.length} 个 Cookie`);

    // 也复制到剪贴板
    navigator.clipboard.writeText(JSON.stringify(cookies, null, 2)).then(() => {
        console.log('✓ Cookie JSON 已自动复制到剪贴板！');
    }).catch(() => {
        console.log('请手动复制上面的 JSON 内容');
    });
})();
