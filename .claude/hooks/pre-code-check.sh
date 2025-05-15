#!/bin/bash
INPUT=$(cat)
TOOL_NAME=$(echo "$INPUT" | jq -r '.tool_name // empty' 2>/dev/null)
if [ "$TOOL_NAME" != "Edit" ] && [ "$TOOL_NAME" != "Write" ]; then exit 0; fi
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty' 2>/dev/null)
SRC_PATTERN="moleculerpy/"
if ! echo "$FILE_PATH" | grep -q "$SRC_PATTERN"; then exit 0; fi
if ! command -v forgeplan &> /dev/null; then exit 0; fi
HEALTH=$(cd "$CLAUDE_PROJECT_DIR" 2>/dev/null && forgeplan health --compact --json 2>/dev/null)
if [ $? -ne 0 ] || [ -z "$HEALTH" ]; then exit 0; fi
ARTIFACT_COUNT=$(echo "$HEALTH" | jq -r '.artifact_count // 0' 2>/dev/null)
if [ "$ARTIFACT_COUNT" = "0" ] || [ -z "$ARTIFACT_COUNT" ]; then exit 0; fi
ACTIVE_PRDS=$(cd "$CLAUDE_PROJECT_DIR" 2>/dev/null && forgeplan list --kind prd --json 2>/dev/null | jq '[.[] | select(.status=="active")] | length' 2>/dev/null)
if [ -z "$ACTIVE_PRDS" ] || [ "$ACTIVE_PRDS" = "0" ]; then
  echo "BLOCKED: No active PRD found."
  exit 2
fi
exit 0
