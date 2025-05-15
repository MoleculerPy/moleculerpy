#!/bin/bash
# PreToolUse hook — блокирует опасные команды
# Matcher: Bash

INPUT=$(cat)
TOOL_NAME=$(echo "$INPUT" | jq -r '.tool_name // empty' 2>/dev/null)
COMMAND=$(echo "$INPUT" | jq -r '.tool_input.command // empty' 2>/dev/null)

if [ "$TOOL_NAME" != "Bash" ] || [ -z "$COMMAND" ]; then
  exit 0
fi

BLOCKED_PATTERNS=(
  "git push --force"
  "git push -f "
  "git reset --hard"
  "git clean -fd"
  "git checkout -- ."
  "rm -rf /"
  "rm -rf ~"
  "rm -rf \$HOME"
  "drop table"
  "DROP TABLE"
  "pip publish"
  "twine upload"
  "python setup.py upload"
)

for pattern in "${BLOCKED_PATTERNS[@]}"; do
  if echo "$COMMAND" | grep -qF "$pattern"; then
    echo "BLOCKED: '$pattern' detected. This is irreversible."
    echo "Use manual terminal if intended."
    exit 2
  fi
done

exit 0
