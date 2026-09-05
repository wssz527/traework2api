#!/usr/bin/env bash
# smoke_fc.sh — tw2api 协议模式日常验收（轻量，消耗约 0.1 work 积分/轮）。
# 用法：scripts/smoke_fc.sh [BASE_URL] [API_KEY]
#   BASE_URL 默认 http://127.0.0.1:7864；API_KEY 默认读 .tw2a_key。
# 项 ①-⑥ 依次验证：健康/模型、普通聊天（fc off 回归）、协议模式流式
# tool_calls、非流式 tool_calls、glm-5.3 协议服从、tool_choice=none 旁路、
# Anthropic /v1/messages 端点回归。
set -uo pipefail

BASE="${1:-http://127.0.0.1:7864}"
KEY="${2:-$(cat "$(dirname "$0")/../.tw2a_key" 2>/dev/null || echo '')}"
AUTH="Authorization: Bearer $KEY"
PASS=0; FAIL=0

ok()   { PASS=$((PASS+1)); echo "  ✓ $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  ✗ $1"; }
check(){ if [ "$1" = "0" ]; then ok "$2"; else bad "$2"; fi; }

echo "== tw2api smoke @ $BASE =="

# ① 健康与模型
curl -sf "$BASE/healthz" >/dev/null; check $? "healthz"
MODELS=$(curl -s "$BASE/v1/models" -H "$AUTH" | python3 -c "import json,sys;print(len(json.load(sys.stdin)['data']))" 2>/dev/null)
[ "${MODELS:-0}" -gt 0 ] 2>/dev/null; check $? "v1/models ($MODELS models)"

# ② 普通聊天（无 tools，fc off 回归）
R=$(curl -s --max-time 120 "$BASE/v1/chat/completions" -H "$AUTH" -H "Content-Type: application/json" \
  -d '{"model":"DeepSeek-V4-Flash-Official","stream":false,"messages":[{"role":"user","content":"Reply with exactly one word: pong"}]}' \
  | python3 -c "import json,sys;print(json.load(sys.stdin)['choices'][0]['message']['content'])" 2>/dev/null)
echo "$R" | grep -qi "pong"; check $? "plain chat (fc off): $(echo "$R" | head -c 40)"

# ③ 协议模式流式：期望 tool_calls + finish_reason
cat > /tmp/smoke_fc_req.json <<'EOF'
{"model":"DeepSeek-V4-Flash-Official","stream":true,
 "tools":[{"type":"function","function":{"name":"Read","description":"Read a file from the local filesystem","parameters":{"type":"object","properties":{"file_path":{"type":"string"}},"required":["file_path"]}}}],
 "messages":[{"role":"user","content":"Use the Read tool to read /etc/hostname, then tell me its exact content."}]}
EOF
R=$(curl -sN --max-time 240 "$BASE/v1/chat/completions" -H "$AUTH" -H "Content-Type: application/json" \
  -d @/tmp/smoke_fc_req.json | grep -c '"finish_reason":"tool_calls"')
[ "${R:-0}" -ge 1 ] 2>/dev/null; check $? "fc stream tool_calls (grep=$R)"

# ④ 非流式 + tools
sed 's/"stream":true/"stream":false/' /tmp/smoke_fc_req.json > /tmp/smoke_fc_ns.json
R=$(curl -s --max-time 240 "$BASE/v1/chat/completions" -H "$AUTH" -H "Content-Type: application/json" \
  -d @/tmp/smoke_fc_ns.json \
  | python3 -c "import json,sys;d=json.load(sys.stdin);c=d['choices'][0];print(c['finish_reason'],c['message']['tool_calls'][0]['function']['name'])" 2>/dev/null)
echo "$R" | grep -q "tool_calls Read"; check $? "fc non-stream tool_calls: $R"

# ⑤ glm-5.3 协议服从（另一家模型的协议遵循度）
sed 's/DeepSeek-V4-Flash-Official/glm-5.3/' /tmp/smoke_fc_req.json > /tmp/smoke_fc_glm.json
R=$(curl -sN --max-time 240 "$BASE/v1/chat/completions" -H "$AUTH" -H "Content-Type: application/json" \
  -d @/tmp/smoke_fc_glm.json | grep -c '"finish_reason":"tool_calls"')
[ "${R:-0}" -ge 1 ] 2>/dev/null; check $? "glm-5.3 fc compliance (grep=$R)"

# ⑥ tool_choice=none：带 tools 但禁用 → 纯文本（不注入协议头）
R=$(curl -s --max-time 120 "$BASE/v1/chat/completions" -H "$AUTH" -H "Content-Type: application/json" \
  -d '{"model":"DeepSeek-V4-Flash-Official","stream":false,"tool_choice":"none","tools":[{"type":"function","function":{"name":"Read","parameters":{"type":"object"}}}],"messages":[{"role":"user","content":"Reply with exactly one word: pong"}]}' \
  | python3 -c "import json,sys;d=json.load(sys.stdin);c=d['choices'][0];print(c['finish_reason'],bool(c['message'].get('tool_calls')))" 2>/dev/null)
echo "$R" | grep -q "stop False"; check $? "tool_choice=none bypass: $R"

# ⑦ Anthropic /v1/messages 回归（无 tools）
R=$(curl -s --max-time 120 "$BASE/v1/messages" -H "$AUTH" -H "Content-Type: application/json" \
  -d '{"model":"DeepSeek-V4-Flash-Official","max_tokens":64,"messages":[{"role":"user","content":"Reply with exactly one word: pong"}]}' \
  | python3 -c "import json,sys;d=json.load(sys.stdin);print(d.get('type'),d.get('stop_reason'))" 2>/dev/null)
echo "$R" | grep -q "message"; check $? "anthropic /v1/messages: $R"

echo "== PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" = "0" ]
