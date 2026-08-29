# traework2api

TRAE Work (SOLO CN) 的 OpenAI 兼容反向代理。把 TRAE SOLO 免费对话通道
（`llm_utils_chat` + `function=solo_work_lite`）包装成标准的
`/v1/chat/completions` + `/v1/models` 接口，支持多账号轮转、自动签到、token 自动刷新。

纯 Go 标准库，零第三方依赖。

## 功能

- **OpenAI 兼容 API**：`POST /v1/chat/completions`（流式/非流式）、`GET /v1/models`
- **多账号池**：积分降序挑选，1005/429/401/5xx 自动冷却、禁用、轮转
- **自动签到**：每日定时签到 + 手动批量签到（`signin.sh`）
- **积分查询**：全账号/指定账号日报（`credit.sh`）
- **Token 自动刷新**：过期前 24h 预刷新，refreshToken 轮换落盘
- **登录闭环**：`login.sh` 自生成登录链接 → 浏览器登录 → 粘贴回调链接 → 换 token 落盘

## 快速开始（Docker）

```bash
# 1. 准备凭证目录（放 trae-*.json）
mkdir -p auths data

# 2. 配置 API Key（Bearer 鉴权，key 只走 env，不落盘 git）
cp .env.example .env
# 编辑 .env，把 changeme 换成你自己的随机密钥

# 3. 启动
docker compose up -d --build

# 4. 验证
curl http://127.0.0.1:7864/healthz          # → ok
curl http://127.0.0.1:7864/v1/models         # → 模型列表
curl http://127.0.0.1:7864/status            # → 账号状态

# 5. 对话
curl -X POST http://127.0.0.1:7864/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${TW2A_API_KEY}" \
  -d '{"model":"glm-5.2","messages":[{"role":"user","content":"你好"}]}'
```

## 登录流程

TRAE 登录页强制回调 `127.0.0.1`，但浏览器与服务器不需要同机：
`login.sh` 自生成登录链接，登录成功后你把地址栏回调链接粘回去即可。

```bash
# 1. 在服务器（或任意机器）运行 login.sh
./login.sh
#    → 打印登录链接（带 127.0.0.1 回调 + 新的 machine/device id）

# 2. 用浏览器打开链接登录（手机号/验证码）
#    登录成功后浏览器跳到打不开的 127.0.0.1 地址

# 3. 复制浏览器地址栏的完整回调链接，粘贴到 login.sh
#    → 解析 refreshToken/userInfo → ExchangeToken 换 token → GetUserInfo 拿 uid
#    → 落盘 auths/trae-{uid}.json → 自动签到 + 查积分 → 重启容器加载新账号
```

## 本机运行（非 Docker）

```bash
# 依赖 Go 1.22+
export TW2A_API_KEY=你的密钥
go build -o tw2api ./cmd/server
./tw2api          # 监听 :7864，auths/ 目录读取凭证
```

配置可选 `config.json`（参考 `config.example.json`），全部项可用 `TW2A_*` env 覆盖：
`TW2A_LISTEN` / `TW2A_AUTH_DIR` / `TW2A_STATE_FILE` / `TW2A_DEFAULT_MODEL` /
`TW2A_PLAN_CREDIT` / `TW2A_SOFT_RATE` / `TW2A_ERR_THRESHOLD` / `TW2A_ERR_COOLDOWN` /
`TW2A_CHECKIN_HOUR` / `TW2A_TIMEOUT_SECONDS`。`TW2A_API_KEY` **只能**从 env 读。

## 运维

```bash
./signin.sh             # 批量签到（全账号，自动 refresh 过期 token）
./credit.sh             # 积分日报（美化）
./credit.sh -json       # 积分日报（JSON）
./credit.sh <uid>       # 指定账号
```

## 签到设备ID

签到接口要求请求头携带**独立设备三件套**（`x-device-id` / `x-device-brand` /
`x-device-type`），与模型通道的 `machineId`/`deviceId` 不是同一组；且**多账号
必须各不相同**，共用同一设备 ID 会被上游关联。凭证字段为 auth 段的
`checkinDeviceId` / `checkinDeviceBrand` / `checkinDeviceType`。

**自动获取（默认，推荐）**：`signin.sh` 或自动签到调度器发现凭证缺
`checkinDeviceId` 时，会自动从**本机已登录的 TRAE SOLO 官方客户端**本地数据
读取设备标识（macOS / Windows），并写回凭证文件，之后容器内调度器直接复用。
可用 `TW2A_DISABLE_CHECKIN_DETECT=1` 关闭该探测。

**手动获取**（账号的官方客户端登录在**另一台设备**上时，需到那台设备上取）：

```bash
# macOS（在那台设备上执行）：
strings ~/Library/Application\ Support/TRAE\ SOLO\ CN/Local\ Storage/leveldb/*.log \
  | grep -o '"user_unique_id":"[0-9]*"' | tail -1   # → checkinDeviceId
sysctl -n hw.model                                   # → checkinDeviceBrand
# checkinDeviceType 填 "mac"
```

```powershell
# Windows PowerShell（在那台设备上执行）：
Get-ChildItem "$env:APPDATA\TRAE SOLO CN\Local Storage\leveldb" -Include *.log,*.ldb -Recurse |
  Select-String '"user_unique_id":"(\d+)"' | Select-Object -Last 1  # → checkinDeviceId
(Get-CimInstance Win32_BaseBoard).Product                            # → checkinDeviceBrand
# checkinDeviceType 填 "windows"
```

把三个值填进 `auths/trae-<uid>.json` 的 `auth` 段即可。既无本机客户端、
凭证也缺字段时，签到会被**明确跳过并打印原因**——不会发无设备头的无效请求，
也不会生成上游无法识别的随机假 ID。

## 目录结构

```
cmd/server/       HTTP 服务（config + main）
cmd/signin/       批量签到工具
cmd/credit/       积分查询工具
internal/auth/    auth 文件解析/原子写回
internal/upstream/ SOLO 上游客户端 + SSE 转换
internal/pool/    账号池（冷却/禁用/积分）
internal/scheduler/ 定时签到 + token 预刷新
internal/server/  OpenAI 兼容路由
login.sh / signin.sh / credit.sh  运维脚本
auths/            （gitignored）trae-*.json 凭证
data/             （gitignored）state.json 池状态
```

## 脱敏说明

- 任何真实 token/key 一律 `********` 或 env 引用，绝不落盘 git。
- `auths/`、`data/`、`config.json`、`.env`、`*.key`、`*.pem` 全部 gitignored。
- `docs/` 不参与上传（gitignore + dockerignore）。
- 验证类文档（`VERIFICATION.md` 等）不入库。
- 日志/状态输出只显示 UID/Nickname/积分，不打印 token。
