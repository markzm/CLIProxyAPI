# CLIProxyAPI 客户端限流与动态并发排队插件 (Rate Limit & Dynamic Queue Plugin)

这是一个 CLIProxyAPI 原生动态链接库插件（C-ABI 插件），**不修改 CLIProxyAPI 主程序代码**，仅通过公开的插件接口（`sdk/pluginabi`、`sdk/pluginapi`）实现按客户端类型的并发限流、排队等待与容量动态调配。

插件及其监控脚本统一放在 `custom/plugins/ratelimit/`（上游项目不存在的目录），合并上游代码时不会产生冲突。它是独立的 Go module，主程序的 `go build ./...` / `go test ./...` 不会包含它。

> 原生插件是**进程内受信任代码**：宿主通过 `dlopen` 加载到同一进程，没有沙箱隔离。插件崩溃会影响整个服务。

---

## 一、核心特性

1. **按客户端类型分流**：通过 `User-Agent` 指纹（Codex、Claude Code、Gemini/Antigravity、IDE 扩展、脚本）或显式 `X-Client-Type` / `X-Caller-Type` 标头分类。
   - 只有 `types` 中**已配置**的类型名会被采用；其余名称一律归入 `default` 桶，客户端无法通过伪造标头创建新的独立配额。
2. **按渠道有效凭据数动态调容**：通过宿主回调 `host.auth.list` 统计各渠道的可用凭据（排除 `disabled`、`unavailable`、状态为 `error`/`disabled` 的凭据），计算：
   $$\text{EffectiveCapacity} = \text{ActiveAuthCount} \times \text{PerAuthConcurrency}$$
   - 读取失败时保留上一次成功的快照（指标中 `auth_sync.stale=true`）。
   - 无可用凭据时按 1 个账号计算，使请求仍能到达宿主并得到宿主自身的错误响应。
3. **FIFO 排队，满载才拒绝**：超出并发容量或滑动窗口配额的请求进入该类型的 FIFO 队列等待；仅在队列满、全局排队预算耗尽或等待超时（`queue_timeout_seconds`）时返回 `429 Too Many Requests`（`Retry-After` 按窗口剩余时间计算）。
4. **客户端断开即退出队列**：宿主在客户端断开后会发送 `request.complete`，插件据此立即将该请求移出队列，不会占用并发配额。
5. **全局排队预算**：每个排队请求都会占用宿主的一个原生调用（即一个 OS 线程），`max_total_waiters` 与 `max_queued_bytes` 限制所有类型合计的排队数量与请求字节数，避免线程耗尽导致进程崩溃。
6. **配置热更新不丢请求**：重新加载配置时复用已有队列，排队中的请求与滑动窗口历史均保留；所有限额在同一把锁内一次性生效。
7. **带鉴权的实时指标**：指标端点需要插件自身配置的 `metrics_token`。

---

## 二、配置说明 (`config.yaml`)

```yaml
plugins:
  enabled: true
  dir: "plugins"
  configs:
    ratelimit:
      enabled: true                 # 由宿主管理的启用开关

      metrics_token: "change-me"    # 指标端点 Bearer Token；为空时指标端点返回 403
      max_total_waiters: 1000       # 全部类型合计的最大排队请求数（每个占用一个 OS 线程）
      max_queued_bytes: 268435456   # 全部排队请求的最大请求体总字节数

      channel_per_auth:             # 每个有效凭据提供的并发数（按渠道）
        codex: 5
        claude: 3
        gemini: 10
        xai: 4
      default_channel_per_auth: 2

      queue_per_auth:               # 每个有效凭据对应的队列长度（别名：channel_queue_per_auth）
        codex: 100
      default_queue_per_auth: 100

      window_per_auth: {}           # 每个有效凭据的滑动窗口请求数（别名：channel_window_per_auth）
      window_minutes: 10            # 全局滑动窗口时长
      window_max_requests: 0        # 全局窗口上限（0 = 关闭）
      auth_sync_interval_seconds: 10

      types:                        # 只需写要覆盖的字段，其余字段保留内置默认值
        codex:
          max_queue_size: 50        # 队列长度上限（与按凭据计算的值取较小者）
          queue_timeout_seconds: 45
        script-bot:
          static_max_concurrency: 2 # 固定并发上限，不随凭据数变化

      default_rule:                 # 未配置类型及 default 桶使用的规则
        max_queue_size: 30
        queue_timeout_seconds: 30
```

内置类型：`codex`(渠道 codex)、`claude-code`(claude)、`gemini`(gemini)、`ide-plugin`(codex)、`script-bot`(default)。在 `types` 中新增的类型会先继承 `default_rule`，再应用自身字段。

### 生效优先级

| 限额 | 优先级（从高到低） |
| --- | --- |
| 并发 | `static_max_concurrency` > `channel_per_auth[渠道]`（或 `default_channel_per_auth`）× 凭据数 |
| 队列长度 | `static_max_queue_size` > (`types.<t>.queue_per_auth` > `queue_per_auth[渠道]` > `default_queue_per_auth`) × 凭据数，再以 `max_queue_size` 封顶 |
| 窗口上限 | `static_window_max_requests` > (`types.<t>.window_per_auth` > `window_per_auth[渠道]` > `default_window_per_auth`) × 凭据数 > `types.<t>.window_max_requests` > `window_max_requests` |
| 窗口时长 | `types.<t>.window_minutes` > `window_minutes` > 10 |
| 排队超时 | `types.<t>.queue_timeout_seconds` > `default_rule.queue_timeout_seconds` > 600 |

`queue_per_auth` 与 `channel_queue_per_auth` 同时设置同一渠道时，以 `channel_queue_per_auth` 为准。负数或超出范围的值会导致配置加载失败，而不是被静默替换。

---

## 三、编译与部署

```bash
mkdir -p /data/CLIProxyAPI/plugins
cd /data/CLIProxyAPI/custom/plugins/ratelimit/go
go build -buildmode=c-shared -o /data/CLIProxyAPI/plugins/ratelimit.so .
rm -f /data/CLIProxyAPI/plugins/ratelimit.h
```

macOS 生成 `ratelimit.dylib`，Windows 生成 `ratelimit.dll`。每次升级主程序后需重新编译插件并运行测试：

```bash
cd custom/plugins/ratelimit/go && go test -race ./...
```

---

## 四、监控端点

```bash
curl -H "Authorization: Bearer <metrics_token>" \
  http://localhost:8317/v0/resource/plugins/ratelimit/ratelimit/metrics

RATELIMIT_METRICS_TOKEN=<metrics_token> python3 custom/plugins/ratelimit/scripts/monitor_queue.py
```

宿主不会对插件 resource 路由做管理鉴权，因此由插件自行校验 `metrics_token`：未配置时返回 403，Token 错误时返回 401。

返回内容包括：
- `ratelimit_metrics.<类型>`：`channel`、`in_flight_requests`、`queue_depth`、`effective_capacity`、`effective_max_queue`、`active_auth_count`（按该类型的渠道统计）、`admitted_requests`、`queued_requests`、`rejected_queue_full`、`rejected_global_limit`、`rejected_timeout`、`canceled_in_queue`、`avg_wait_ms`/`max_wait_ms`（仅统计排队后被准入的请求）、窗口指标。
- `global`：全局排队数与字节数及其上限、登记中的请求数、`plugin_errors`。
- `auth_sync`：各渠道凭据数、同步时间、是否为过期快照及错误信息。

---

## 五、已知限制与运维建议

- **排队占用线程**：C ABI 调用是同步的，每个排队请求都会占用一个 OS 线程，直到被准入、超时或取消。请按机器能力设置 `max_total_waiters`（远低于 Go 运行时 10000 线程上限）。高峰过后 Go 运行时会保留部分空闲线程以便复用，线程数不会立即回落。
- **失败放行 (fail-open)**：插件返回错误或被宿主熔断时，宿主会跳过该插件并放行请求。插件内部错误计入 `global.plugin_errors`。
- **按进程、按客户端类型限流**：不是按实际选中的凭据限流，也不跨多实例共享；同一渠道的多个类型（如 `codex` 与 `ide-plugin`）各自拥有独立容量。
- **客户端类型可声明**：`X-Client-Type` 与 `User-Agent` 由客户端提供，只能在已配置类型之间切换，不应视为身份认证。
- **超时设置**：`queue_timeout_seconds` 应小于下游客户端与反向代理的超时时间，否则客户端会先于插件放弃等待。
- **重载/关闭**：宿主在关闭插件前会等待在途调用结束，排队中的请求会在被准入或超时后才返回。
