import { withMermaid } from 'vitepress-plugin-mermaid';

const config = withMermaid({
  lang: 'zh-CN',
  title: 'DogX 文档',
  description: 'DogX 软件设计、架构决策与开发规范。',
  cleanUrls: true,
  head: [['link', { rel: 'icon', href: '/favicon.ico' }]],
  mermaid: {
    // 文档图表不需要宽松 HTML 或点击脚本能力，覆盖插件的 loose 默认值。
    securityLevel: 'strict',
    startOnLoad: false,
  },
  vite: {
    // 插件动态注入 Mermaid 入口，显式预构建以转换其 fastdom 等 CommonJS 依赖。
    optimizeDeps: { include: ['mermaid'] },
    server: {
      host: '127.0.0.1',
      port: 5173,
      strictPort: true,
    },
  },
  themeConfig: {
    nav: [
      { text: '首页', link: '/' },
      { text: '软件设计', link: '/design/scheduler/', activeMatch: '/design/' },
      {
        text: '开发规范',
        link: '/development/documentation',
        activeMatch: '/development/',
      },
      {
        text: '架构决策',
        link: '/adr/0001-dogx-v0.1-architecture',
        activeMatch: '/adr/',
      },
    ],
    sidebar: [
      {
        text: '定时任务设计说明书',
        collapsed: false,
        items: [
          { text: '总览与阅读指南', link: '/design/scheduler/' },
          { text: '1 目标与范围', link: '/design/scheduler/scope' },
          { text: '2 总体架构', link: '/design/scheduler/architecture' },
          { text: '3 任务模型', link: '/design/scheduler/task-model' },
          { text: '4 调度与执行规则', link: '/design/scheduler/behavior' },
          { text: '5 执行流程与可靠性', link: '/design/scheduler/execution' },
          { text: '6 数据与接口契约', link: '/design/scheduler/data-contracts' },
          { text: '7 管理与运维', link: '/design/scheduler/operations' },
          { text: '8 验证与交付', link: '/design/scheduler/verification' },
          { text: '设计进度与评审计划', link: '/design/scheduler/progress' },
          {
            text: '触发模块开工总审',
            link: '/design/scheduler/trigger-readiness-review',
            items: [
              { text: 'SQL 对照稿', link: '/design/scheduler/trigger-sql-review' },
              { text: 'HTTP 与 RPC 对照稿', link: '/design/scheduler/trigger-interface-review' },
            ],
          },
          { text: '参考资料与决策索引', link: '/design/scheduler/references' },
        ],
      },
      {
        text: '项目规划',
        items: [
          { text: 'v0.1 功能边界', link: '/roadmap/v0.1' },
        ],
      },
      {
        text: '开发规范',
        items: [
          { text: '文档站使用', link: '/development/documentation' },
          { text: '代码生成', link: '/development/code-generation' },
          { text: '数据库迁移', link: '/development/database-migrations' },
          { text: '数据库索引', link: '/development/database-indexes' },
          { text: '测试方案', link: '/development/testing' },
        ],
      },
      {
        text: '架构决策 ADR',
        collapsed: false,
        items: [
          { text: '0001 · 总体架构', link: '/adr/0001-dogx-v0.1-architecture' },
          { text: '0002 · 响应与错误', link: '/adr/0002-http-response-and-rpc-errors' },
          { text: '0003 · 认证与会话', link: '/adr/0003-authentication-and-session' },
          { text: '0004 · 单租户 RBAC', link: '/adr/0004-single-tenant-rbac' },
          { text: '0005 · 权限策略同步', link: '/adr/0005-casbin-runtime-and-policy-sync' },
          { text: '0006 · 刷新令牌轮换', link: '/adr/0006-refresh-rotation-and-browser-sync' },
          { text: '0007 · 字典管理与缓存', link: '/adr/0007-dictionary-management-and-cache' },
          { text: '0008 · 登录保护', link: '/adr/0008-login-protection-and-security-settings' },
          { text: '0009 · 调度域与 etcd', link: '/adr/0009-scheduler-domain-and-etcd' },
        ],
      },
    ],
    outline: { level: [2, 3], label: '本页目录' },
    search: {
      provider: 'local',
      options: {
        locales: {
          root: {
            translations: {
              button: { buttonText: '搜索文档', buttonAriaLabel: '搜索文档' },
              modal: {
                noResultsText: '没有找到相关结果',
                resetButtonTitle: '清除搜索',
                backButtonTitle: '关闭搜索',
                displayDetails: '显示详细列表',
                footer: {
                  selectText: '选择',
                  navigateText: '切换',
                  closeText: '关闭',
                },
              },
            },
          },
        },
      },
    },
    socialLinks: [{ icon: 'github', link: 'https://github.com/tokyolab/dogx' }],
    docFooter: { prev: '上一页', next: '下一页' },
    sidebarMenuLabel: '文档导航',
    returnToTopLabel: '返回顶部',
    darkModeSwitchLabel: '切换主题',
    darkModeSwitchTitle: '切换到深色模式',
    lightModeSwitchTitle: '切换到浅色模式',
    notFound: {
      title: '页面未找到',
      quote: '可以通过文档导航或搜索查找内容。',
      linkLabel: '返回首页',
      linkText: '返回首页',
    },
  },
});

// 插件 2.0.17 仍强制预构建 debug，当前 Mermaid 11 依赖图已不包含它。
const optimizeDeps = config.vite?.optimizeDeps;
if (optimizeDeps?.include) {
  optimizeDeps.include = optimizeDeps.include.filter((name) => name !== 'debug');
}

export default config;
