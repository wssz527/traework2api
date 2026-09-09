# trae — CLIProxyAPI 插件

把 [traework2api](https://github.com/)（独立 Go 服务，监听 `:7864`）迁移为 CLIProxyAPI（CPA）**自定义上游 provider 插件**。
产物 `trae.dylib`，由 CPA 宿主 `dlopen` 加载；`POST /v1/chat/completions` 由宿主自己持有，插件只提供 executor / auth / model / scheduler / management 能力。

形态与 workbuddy 插件同款（照抄其工程结构、C ABI 样板与测试布局）。

> 完整迁移方案见 `work/minimax/trae-cpa-migration.md`（374 行，含 CPA 插件契约、模块去留映射、凭证迁移、割接方案、风险清单）。本文档只写落地相关的契约与操作。

---

## 1. 插件契约

### 导出符号（ABI v1 / Schema v1）

```
cliproxy_plugin_init  cliproxyPluginCall  cliproxyPluginFree  cliproxyPluginShutdown
```

- `cliproxyPluginShutdown` 是**故意的 no-op**：宿主在自己 runtime teardown 阶段调用它，此时触碰 Go 同步原语会 SIGSEGV（workbuddy 实测每次 docker restart 必现）。
- 调度器 goroutine 在 `plugin.register/reconfigure` 时由 `ensureScheduler()` 幂等启动，靠进程退出回收。

### Capability

| Capability | 值 | 说明 |
|---|---|---|
| `model_provider` | ✓ | `model.static`（32 静态 config_name）/ `model.for_auth`（动态 + 缓存） |
| `auth_provider` | ✓ | `auth.parse` / `auth.refresh` |
| `executor` | ✓ | scope=`oauth`，输入/输出格式 `chat-completions` |
| `scheduler` | ✓ | `scheduler.pick`，默认 `off`（交给 CPA 内建） |
| `management_api` | ✓ | 路由 + `/panel` 页面资源 |
| `frontend_auth_provider` | ✗ | 不需要 |
| `usage_plugin` | ✗ | c-shared 有独立 Go runtime，写宿主 `usage.DefaultManager` 无效 |

### 红线：panic 隔离

c-shared **没有进程隔离**——一个 panic 会拖垮整个 CPA 进程（8317 上所有 provider 一起中断）。

- 所有 RPC 入口统一走 `handleMethodGuarded`，`defer recover()` 把 panic 转成 error envelope。
- 所有自起 goroutine（stream pump、调度器、签到/刷新 fan-out）内部各自 `defer recover()`。
- `pluginLogf` 输出前必须过 `redactSecrets`。

### 网络

**macOS 禁用 `host.http.do`**（nested-RPC 期间宿主栈移动会让响应指针悬空，workbuddy 在 `model.for_auth` 实测触发过）。全部上游调用走插件内 `net/http`，共享连接池：

- `HTTP`：短 JSON 请求，120s 总超时
- `StreamHTTP`：SSE 长流，无总超时，靠 `ResponseHeaderTimeout=300s` 兜首字节（长上下文首包可能需 1-2 分钟）

副作用：CPA request-log 不记录 Trae 的出站请求，排障靠 `host.log` 或面板。

### 配置项（`plugins.configs.trae`）

| 字段 | 默认 | 说明 |
|---|---|---|
| `checkin_auto` | `true` | 每日自动签到 |
| `checkin_hour` | `9` | 签到本地小时（对齐原 traework2api 的 9 点档） |
| `refresh_hours` | `[3]` | token 预刷新本地小时 |
| `refresh_skew` | `24h` | 预刷新窗口 |
| `lifecycle_auto` | `true` | 冷却/禁用自动生效 |
| `scheduler_mode` | `off` | `off`=交给 CPA 内建；`credits`=插件挑号（面板选中 + 粘性） |
| `version_track` | `true` | 上游客户端版本自动跟踪（见 §2.1） |
| `version_track_interval` | `24h` | 版本探测间隔（Go duration） |

### 2.1 上游客户端版本自动跟踪

上游 / TRAE 客户端发新版时插件自己跟上，不需要手动改 `constants.go`。

**三级回退**：

1. **公开网页源**（默认 24h 探一次）：`https://www.trae.cn/changelog`、`https://docs.trae.cn/work_changelog`，两个都试，取解析成功且版本最大的那个
2. **本机客户端探测**：`/Applications/TRAE SOLO CN.app/Contents/Info.plist` 的 `CFBundleShortVersionString`（未安装则跳过）
3. **内置常量**：`constants.go` 的 `IdeVersion` / `IdeVersionCode`（最终回退，也是下限）

**"只进不退"保护（关键）**：实测（2026-09-08）两个网页源都停留在 `0.1.49-52`（2026-08-21），而内置常量与本机客户端都是 `0.1.63`。若网页优先且取到就用，插件会把版本**降**到 0.1.49 —— 那是回退不是跟踪，上游可能据此改变行为。因此内置常量被当作**下限**：取所有来源的最大值，跟踪只向前走。

缓存：成功 24h，失败负缓存 1h。失败静默回退，**绝不阻塞请求链路**（无缓存时先用内置常量，探测在后台 goroutine 做）。加 debug 日志。

解析保守：只接受严格 `\d+\.\d+\.\d+`；只取 `TraeWork` 产品线（页面同时列 TraeCode 3.x 与 TRAE APP 0.0.x）；页面结构变化抓不到就当失败，不产出垃圾值。网页源只有版本号时 `IdeVersionCode` 取页面日期。

请求头 `User-Agent` / `X-Ide-Version` / `X-Ide-Version-Code` / `X-App-Version-Code` 与 `GetUserInfo` 的 `IDEVersion` 都读跟踪结果而非常量。

运维接口：`GET /version`（当前值、内置值、缓存状态、源列表）、`POST /version/refresh`（后台强制重探，不阻塞）。

---

## 2. 凭证布局

```
~/.cli-proxy-api/trae-<uid>.json
```

```jsonc
{
  "type": "trae",              // 必填：宿主按顶层 type 路由；缺失时其它插件可能抢先 Handled=true
  "provider": "trae",
  "disabled": false,
  "note": "昵称",
  "auth": {
    "accessToken": "...", "refreshToken": "...", "expiresAt": 1786805537,
    "domain": "trae.cn", "apiHost": "https://api.trae.com.cn",
    "machineId": "...", "deviceId": "...",
    "checkinDeviceId": "1111222233334444",   // 每账号独立，16 位数字
    "checkinDeviceBrand": "Mac16,10",
    "checkinDeviceType": "mac"
  },
  "account": { "uid": "...", "enterpriseId": "...", "nickname": "..." }
}
```

**迁移规则**（原 `traework2api/auths/trae-<uid>.json` → 新位置）：原样保留 `auth{}` / `account{}` 两层，顶层加 `"type":"trae"`。

**`auth.parse` 硬约束**：返回的 `AuthData.ID` 必须**留空**，让宿主用 `authIDForPath(path)` 计算。设成 `ID=uid` 而宿主 watcher 用 `ID=filename` 时，`upsertAuthRecord` 找不到已有记录 → 新建一条 → **同一文件出现重复 auth 条目**。同时必须回显 `FileName`。

**自定义字段安全性**：`machineId` / `checkinDevice*` 全部放在 `AuthData.StorageJSON`（插件自有的 opaque JSON，宿主不解析），迁移无损。

### 签到设备 ID（防风控，不能丢）

签到接口要求 `x-device-id` / `x-device-brand` / `x-device-type` 三件套，与模型通道的 `machineId/deviceId` 不同。
多账号共用同一设备 ID 时上游只让第一个签成（报 9095），因此每个账号分配一个**固定的 16 位数字** ID（与官方 `user_unique_id` 同构；含字母的 hex 会触发风控 9074），生成一次写回凭证，长期复用。

---

## 3. Management 路由

宿主前缀：`/v0/management/plugins/trae/*`（页面资源 `/v0/resource/plugins/trae/panel`）。

| Method | Path | 说明 |
|---|---|---|
| GET | `/accounts` | 账号列表（UID 脱敏）、积分、冷却/禁用状态、当前选中；每账号带 `credits_detail`（`total_remain`/`total_used`/`total_size`/`pack_count`/`fetched_at`）供面板画用量进度条 |
| POST | `/refresh` | 强制刷新全部账号积分 |
| POST | `/checkin` | 签到：body `{auth_index}` 单号，留空全量 |
| POST | `/checkin/config` | 开关自动签到 / 设 `checkin_hour`（运行时生效，重启回落到 config.yaml） |
| GET | `/credits` | 积分查询：`?auth_index=` 单号，留空全量；带 `credits_total`/`credits_used`/`pack_count`/`credits_detail` 用量明细 |
| POST | `/import` | 导入凭证 JSON（嵌套/扁平均可）到宿主 auth store |
| POST | `/select` | 切换 active auth（chat 路由优先账号） |
| POST | `/keepalive` | 手动刷新 token（单号或全量） |
| GET | `/status` | 调度器与生命周期配置，含 `server_time`（`YYYY-MM-DD HH:MM:SS`） |
| GET | `/version` | 上游版本跟踪：当前值、内置值、缓存状态、源列表 |
| POST | `/version/refresh` | 强制重新探测上游版本（后台执行，不阻塞） |

```bash
KEY=$(cat ~/.cli-proxy-api/.mgmt_key)
BASE=http://127.0.0.1:8317/v0/management/plugins/trae

curl -s "$BASE/accounts" -H "Authorization: Bearer $KEY" | jq
curl -s -X POST "$BASE/checkin" -H "Authorization: Bearer $KEY" -d '{}' | jq
curl -s "$BASE/credits" -H "Authorization: Bearer $KEY" | jq
```

面板：`open http://127.0.0.1:8317/v0/resource/plugins/trae/panel`

---

## 4. 构建与测试

工具链 `~/go-toolchain/go`（go1.26.6 darwin/arm64）。

```bash
export PATH=~/go-toolchain/go/bin:$PATH
make build     # → trae.dylib（buildmode=c-shared, CGO_ENABLED=1）
make test      # go test -race -count=1 ./...
make lint      # gofmt + go vet
```

当前状态：`make build` 通过（7.0 MB，四个 ABI 符号齐全）；`go test -race ./...` 通过，145 个测试；`go vet` / `gofmt` 干净。

测试全部使用**合成假数据**（`at-placeholder` / `rt-placeholder`），不读 `~/.cli-proxy-api/trae-*.json`，不把 token 写进断言或输出。

---

## 5. 与迁移方案的偏差

| # | 方案原文 | 实际做法 | 理由 |
|---|---|---|---|
| 1 | 项目放 `~/tw-plugin-build/` | `/Users/wssz277/trae-plugin-build`（与 `wb-plugin-build` 平级） | 按主会话指定的新项目位置 |
| 2 | `Makefile` 照抄，产物 `workbuddy.so` 形态 | 产物 `trae.dylib`（macOS），`release` 只编 linux | 本机是 darwin/arm64，直接在役环境要的是 dylib |
| 3 | 保留 `scheduler.pick`（二期再上） | 已实现，但默认 `scheduler_mode: off` | 实现成本已付；默认仍交给 CPA 内建，行为与方案一致 |
| 4 | `CoolSoft` / `CoolErr` 交给 CPA 内建 | CPA 内建为主 + 插件内 `errCount` 兜底计数 | CPA 内建重试只覆盖单次请求；连续错误阈值在插件内更可控 |
| 5 | `Aggregate` / `StreamWithError` 保留 | `Aggregate` 原样保留；`Stream/StreamWithError` 改为 `collectOpenAISSEChunks` | 插件不持有 `http.ResponseWriter`（宿主持有 HTTP 连接），流式必须返回 chunk 文本给宿主 |
| 6 | usage 走外部上报（CPAMP） | 未实现（`usage_plugin: false`） | 本机无 CPAMP 部署；留到有上报目标时再加 |
| 7 | `login.sh` 保留为独立脚本，改 2 处 | 同目录提供改造版 `login.sh`：改 `AUTH_DIR`、加 `"type":"trae"`、删 docker 段 | 与方案一致；末尾不再 curl `:7864` |
| 8 | 凭证迁移"只复制不删源" | 未执行（由主会话割接时做） | 红线：不得动在役服务与 `~/.cli-proxy-api/` |
| 9 | 版本跟踪（补充需求）：网页源优先、取到即用 | 改为**取所有来源最大值 + 内置常量作下限** | 实测两个网页源停留在 0.1.49-52，而内置/本机是 0.1.63；照原样实现会把版本降级，反而破坏在役链路 |

未改动的上游常量集中在 `constants.go`（原文件注释"禁止改动"），原样搬运。

---

## 6. 割接清单（交给主会话执行）

本仓库**只构建产物**，未做任何运行时变更。以下步骤由主会话执行：

### 阶段 1 — 并行（tw2api :7864 全程不停）

1. 复制插件到 CPA 插件目录：
   ```bash
   cp ~/trae-plugin-build/trae.dylib ~/cliproxyapi/plugins/trae.dylib
   ```
2. 凭证迁移（**只复制，不删源**），为每个文件加顶层 `"type":"trae"`：
   ```bash
   for f in ~/traework2api/auths/trae-*.json; do
     python3 - "$f" <<'PY'
   import json,sys,os,pathlib
   src=sys.argv[1]; d=json.load(open(src)); d["type"]="trae"
   dst=pathlib.Path.home()/".cli-proxy-api"/os.path.basename(src)
   json.dump(d, open(dst,"w"), indent=2, ensure_ascii=False)
   print(dst)
   PY
   done
   ```
   灰度期**只放 1 个账号**（其余 3 个先不放）。
3. `~/cliproxyapi/config.yaml` 加（**改前先备份**）：
   ```yaml
   plugins:
     enabled: true
     dir: plugins
     configs:
       workbuddy:
         enabled: true
         priority: 100
       trae:
         enabled: true
         priority: 90
         checkin_auto: true
         checkin_hour: 9
         refresh_hours: [3]
         scheduler_mode: off
   ```
4. `launchctl kickstart -k gui/$(id -u)/com.cliproxyapi.server`

### 阶段 2 — 验证（双通道并行，零风险）

```bash
KEY=$(cat ~/.cli-proxy-api/.api_key)
# a. 模型表出现 Trae 模型
curl -s http://127.0.0.1:8317/v1/models -H "Authorization: Bearer $KEY" | grep -i deepseek
# b. 非流式
curl -s http://127.0.0.1:8317/v1/chat/completions -H "Authorization: Bearer $KEY" \
  -H 'Content-Type: application/json' \
  -d '{"model":"DeepSeek-V4-Flash-Official","messages":[{"role":"user","content":"ping"}]}'
# c. 流式（SSE 最容易出问题）
curl -N http://127.0.0.1:8317/v1/chat/completions -H "Authorization: Bearer $KEY" \
  -H 'Content-Type: application/json' \
  -d '{"model":"glm-5.2","messages":[{"role":"user","content":"ping"}],"stream":true}'
# d. 面板
open http://127.0.0.1:8317/v0/resource/plugins/trae/panel
# e. 与 :7864 对拍（同 prompt 双发，比对输出与 token 数）
# f. 长上下文回归：复用 ~/cliproxyapi/niah_test.py 128K 档
```

验收线：a–d 全绿，e 输出一致，f 无降智。

### 阶段 3 — 切换

5. 备份 `~/.kimi-code/config.toml` → `.bak-<date>`
6. 改 `[providers.trae]`：`base_url` → `http://127.0.0.1:8317/v1`，`api_key` → CPA 的（`~/.cliproxyapi/.api_key`）
7. 模型 ID 与插件注册 ID 对齐（迁移报告 §4.1 第 2、3 项）
8. 发布剩余 3 个账号到 `~/.cli-proxy-api/`
9. 更新 `login.sh`（用本仓库改造版）、`credit.sh`、`signin.sh` 的路径
10. 观察 24h（覆盖一次 09:00 签到 + 03:00 预刷新）

### 阶段 4 — 下线 :7864

11. `launchctl unload -w ~/Library/LaunchAgents/com.traework2api.server.plist`（**unload 不 delete**，保留回滚）
12. 确认无监听：`lsof -nP -iTCP:7864 -sTCP:LISTEN`
13. `~/traework2api/` 整体归档不删（含 `auths/` 原始凭证）

### 回滚

| 场景 | 动作 |
|---|---|
| 插件有问题 | `config.yaml` 里 `plugins.configs.trae.enabled: false` → kickstart |
| CPA 起不来 | 移走 `plugins/trae.dylib` → kickstart |
| 已切调用方要退回 | 还原 `config.toml`（有 .bak）→ `launchctl load -w` tw2api plist → kickstart |
| 凭证被改坏 | `~/traework2api/auths/` 原文件未删，拷回 |

回滚时间目标 < 2 分钟。

---

## 7. 文件树

```
trae-plugin-build/
├── main.go               C ABI 导出、RPC 分发、registration、auth.parse/refresh
├── version.go            上游客户端版本自动跟踪（三级回退 + 缓存）
├── constants.go          上游技术常量（原样，禁止改动）
├── headers.go            SOLO / ug / oauth 三类请求头
├── payload.go            OpenAI → SOLO 请求体改写
├── client.go             上游客户端（chat/models/checkin/credits/refresh）
├── solosse.go            SOLO SSE 解析 → 聚合 + OpenAI chunk
├── auth.go               凭证解析 / 并发安全 token 读写
├── authfile.go           宿主 auth 读写（host.auth.list/get/save）+ 序列化
├── checkin_device.go     每账号独立签到设备 ID
├── checkin.go            签到执行 + 每账号互斥锁
├── executor.go           executor.execute / execute_stream
├── models.go             model.static / model.for_auth + 缓存 + 别名反解
├── lifecycle.go          冷却/禁用状态机 + 执行结果回写
├── scheduler.go          scheduler.pick + 每日签到/预刷新 goroutine
├── active_auth.go        面板选中账号（粘性路由）
├── management.go         management 路由与处理
├── credits.go            积分缓存 + 账号行结构
├── config.go             plugin.reconfigure 配置解析（含 version_track）
├── panel.go / panel.html 面板页面
├── redact.go             凭证脱敏
├── login.sh              改造版登录脚本
├── Makefile / go.mod / VERSION / .gitignore
├── version_fixtures_test.go  静态 HTML 样例（官方更新日志页片段）
└── *_test.go             145 个测试（合成假数据；单测不依赖真实外网）
```
