#!/bin/bash
# PreToolUse hook — блокирует коммит если новые публичные функции без тестов
# Matcher: Bash (git commit)

INPUT=$(cat)
TOOL_NAME=$(echo "$INPUT" | jq -r '.tool_name // empty' 2>/dev/null)
COMMAND=$(echo "$INPUT" | jq -r '.tool_input.command // empty' 2>/dev/null)

if [ "$TOOL_NAME" != "Bash" ] || [ -z "$COMMAND" ]; then
  exit 0
fi
if ! echo "$COMMAND" | grep -qE "git commit"; then
  exit 0
fi

# Ищем новые публичные функции в staged .py файлах
DIFF=$(cd "$CLAUDE_PROJECT_DIR" && git diff --cached --unified=0 -- '*.py' 2>/dev/null)
if [ -z "$DIFF" ]; then
  exit 0
fi

# Находим новые функции (def, async def) — исключая тесты и приватные (_)
NEW_FNS=$(echo "$DIFF" | grep '^+' | grep -v '^+++' | grep -E '^\+\s*(async\s+)?def [a-zA-Z]' | grep -v 'def test_' | grep -v 'def _')
if [ -z "$NEW_FNS" ]; then
  exit 0
fi

FN_COUNT=$(echo "$NEW_FNS" | wc -l | tr -d ' ')

# Проверяем есть ли новые тесты
NEW_TESTS=$(echo "$DIFF" | grep '^+' | grep -v '^+++' | grep -E '^\+\s*(async\s+)?def test_')
TEST_COUNT=0
if [ -n "$NEW_TESTS" ]; then
  TEST_COUNT=$(echo "$NEW_TESTS" | wc -l | tr -d ' ')
fi

if [ "$TEST_COUNT" -eq 0 ] && [ "$FN_COUNT" -gt 0 ]; then
  echo "BLOCKED: $FN_COUNT new public function(s) but 0 new tests."
  echo ""
  echo "New functions without tests:"
  echo "$NEW_FNS" | head -10 | sed 's/^+/  /'
  echo ""
  echo "Write tests for each new public function before committing."
  exit 2
fi

# Предупреждение если соотношение плохое
if [ "$FN_COUNT" -gt "$TEST_COUNT" ]; then
  echo "WARNING: $FN_COUNT new public functions but only $TEST_COUNT new tests."
  echo "Consider adding more tests. Proceeding anyway."
fi

exit 0
