# CLIProxyAPI 客户端限流与动态并发排队插件 (Rate Limit & Dynamic Queue Plugin)

这是一个专为 CLIProxyAPI 设计的原生动态链接库插件（C-ABI 插件），运行于独立沙箱中，**完全不修改 CLIProxyAPI 开源核心代码**，实现对下游客户端按类型进行细粒度限流、超时排队与容量动态调配。

---

## 一、核心特性

1. **统一 API Key 下的精准分类**：
   - 不依赖多 Key 区分，通过客户端 `User-Agent` 指纹库（Codex 桌面/CLI、Claude Code、Antigravity、IDE 扩展等）及显式 `X-Client-Type` 进行精准分流。
2. **基于渠道 Auth 文件数量动态调容**：
   - 支持配置每个渠道（如 `codex`、`claude`、`gemini`）单个 Auth 文件的并发基数。
   - 插件通过宿主回调 `host.auth.list` 动态感知当前有效凭据数量，自动计算：
     $$\text{EffectiveCapacity} = \text{ActiveAuthCount} \times \text{PerAuthConcurrency}$$
   - 支持 Auth 文件热增删时的平滑扩容与缩容（缩容时不强杀在途请求，扩容时自动唤醒排队队列）。
3. **安全排队与 429 熔断截流**：
   - 超出并发容量的请求进入各类型的 FIFO 排队通道。
   - 若排队超时（`queue_timeout_seconds`）或队列溢出（`max_queue_size`），返回标准 `429 Too Many Requests`（附带 `Retry-After: 5` 标头与 JSON 错误体）。
   - 客户端提前断开连接时，自动退出队列，绝不浪费并发配额。
4. **清晰的结构化日志打点**：
   - 统一输出 `[QUEUE_ENTER]`、`[QUEUE_ACQUIRED]`、`[CONCURRENCY_RELEASED]`、`[RATELIMIT_REJECTED]`、`[CAPACITY_RECALCULATED]` 等全链路日志。
5. **实时监控与诊断端点**：
   - 原生对接 `ManagementAPI`，可通过管理端点获取各客户端类型的在途并发、队列深度、平均排队耗时及拒绝统计。
6. **零侵入开源代码，无痛升级**：
   - 插件独立编译为 `.so`（Linux）、`.dylib`（macOS）或 `.dll`（Windows）。
   - 宿主通过 `dlopen` 动态加载，主仓库执行 `git pull` 升级时 100% 零代码冲突。

---

## 二、配置说明 (`config.yaml`)

在 CLIProxyAPI 的 `config.yaml` 中配置启用该插件：

```yaml
plugins:
  enabled: true
  dir: "plugins"                  # 插件目录
  configs:
    ratelimit:
      enabled: true               # 启用限流插件
      
      # 渠道单 Auth 文件并发基数
      channel_per_auth:
        codex: 5                  # 每个有效 codex 凭据提供 5 并发
        claude: 3                 # 每个有效 claude 凭据提供 3 并发
        gemini: 10                # 每个有效 gemini 凭据提供 10 并发
        xai: 4                    # 每个有效 xai 凭据提供 4 并发
      
      default_channel_per_auth: 2 # 未声明渠道默认每个凭据 2 并发
      auth_sync_interval_seconds: 10 # 动态探测 auth 文件变更的时间间隔(秒)

      # 客户端类型规则
      types:
        codex:
          channel: "codex"
          max_queue_size: 50
          queue_timeout_seconds: 45
        
        claude-code:
          channel: "claude"
          max_queue_size: 30
          queue_timeout_seconds: 30

        gemini:
          channel: "gemini"
          max_queue_size: 50
          queue_timeout_seconds: 30

        ide-plugin:
          channel: "codex"
          max_queue_size: 30
          queue_timeout_seconds: 30

        script-bot:
          channel: "default"
          static_max_concurrency: 2 # 爬虫/自动化脚本固定硬并发上限
          max_queue_size: 100
          queue_timeout_seconds: 60

      default_rule:
        channel: "default"
        max_queue_size: 30
        queue_timeout_seconds: 30
```

---

## 三、编译与部署指南

### 1. 编译动态链接库

在 Linux 环境下，进入插件目录执行编译：

```bash
mkdir -p /data/CLIProxyAPI/plugins
cd /data/CLIProxyAPI/examples/plugin/ratelimit/go
go build -buildmode=c-shared -o /data/CLIProxyAPI/plugins/ratelimit.so .
rm -f /data/CLIProxyAPI/plugins/ratelimit.h
```

*(Windows 环境编译生成 `ratelimit.dll`，macOS 编译生成 `ratelimit.dylib`)*

### 2. 运行与验证

启动 CLIProxyAPI：
```bash
./cli-proxy-api --config config.yaml
```
服务启动时会自动扫描 `plugins/` 目录加载 `ratelimit.so`，控制台将输出插件注册成功日志。

---

## 四、监控端点查询

当插件启用后，可以通过管理端点查询各客户端的实时指标：

```bash
curl -H "Authorization: Bearer <MANAGEMENT_PASSWORD>" \
  http://localhost:8080/v0/resource/plugins/ratelimit/ratelimit/metrics
```

返回示例：
```json
{
  "ratelimit_metrics": {
    "codex": {
      "in_flight_requests": 3,
      "queue_depth": 0,
      "total_requests": 128,
      "admitted_requests": 120,
      "queued_requests": 8,
      "rejected_queue_full": 0,
      "rejected_timeout": 0,
      "canceled_in_queue": 0,
      "avg_wait_ms": 142.5,
      "max_wait_ms": 450,
      "active_auth_count": 3,
      "effective_capacity": 15
    },
    "script-bot": {
      "in_flight_requests": 2,
      "queue_depth": 5,
      "total_requests": 500,
      "admitted_requests": 480,
      "queued_requests": 85,
      "rejected_queue_full": 15,
      "rejected_timeout": 5,
      "canceled_in_queue": 0,
      "avg_wait_ms": 1200.0,
      "max_wait_ms": 15000,
      "active_auth_count": 1,
      "effective_capacity": 2
    }
  },
  "timestamp": "2026-10-12T01:35:00Z"
}
```
