# MoviePilot-Plugins

V3 与 V2 分别提供同名插件，配置与用法相同。

## 神医媒体订阅请求插件

接收神医通过 Emby Webhook 发出的 `library.subscribe` 事件，并按事件中的TMDB/豆瓣标识创建订阅。

### 配置

- **前置要求**：MoviePilot设置中配置并启用了至少一个Emby媒体服务器
- **启用插件**：开启后处理 Webhook
- **创建订阅后发送通知**：创建成功时发送通知
- **重复事件忽略窗口**：同一部影片/剧集（按TMDB/豆瓣标识和季号判断）在窗口内重复收到时跳过，默认为300秒

在Emby中启用神医的`媒体订阅请求`Webhook通知，请求内容类型设为`multipart/form-data`，并将地址指向：

```text
http://<地址>:3001/api/v1/webhook/?token=<API_TOKEN>
```

### 规则

- 使用 `Item.ProviderIds.DiscoverItemType` 判断电影/电视剧；不使用 `Item.Type`，因为探索频道条目的类型是 `Trailer`
- 优先使用权威的 `Item.ProviderIds.Douban` 创建订阅；豆瓣识别失败时才回退 `Item.ProviderIds.Tmdb`
- 从 `Item.Name` 解析季号，兼容 `第二季`、`Season 2`、`S2` 等写法
- 使用 `exist_ok=True` 创建订阅，并在插件详情页保留最近 100 条处理记录
