#!/bin/bash
# PreToolUse hook — проверяет синхронность версий при PR и push
# Matcher: Bash
# Triggers on: gh pr create, git push, git tag

INPUT=$(cat)
COMMAND=$(echo "$INPUT" | jq -r '.command // empty' 2>/dev/null)

# Только для PR create, push, tag
if ! echo "$COMMAND" | grep -qE "gh pr create|git push|git tag"; then
    exit 0
fi

PROJECT_DIR="${CLAUDE_PROJECT_DIR:-$(pwd)}"

# Ищем pyproject.toml
PYPROJECT="$PROJECT_DIR/pyproject.toml"
INIT_PY="$PROJECT_DIR/moleculerpy/__init__.py"
README="$PROJECT_DIR/README.md"

if [ ! -f "$PYPROJECT" ]; then
    exit 0
fi

# Извлекаем версии
VER_PYPROJECT=$(grep '^version = ' "$PYPROJECT" | head -1 | sed 's/version = "\(.*\)"/\1/')
VER_INIT=$(grep '__version__ = "' "$INIT_PY" 2>/dev/null | head -1 | sed 's/.*__version__ = "\(.*\)"/\1/')
VER_README=$(grep 'Current status (v' "$README" 2>/dev/null | head -1 | sed 's/.*Current status (v\(.*\))/\1/')

ERRORS=""

if [ -n "$VER_INIT" ] && [ "$VER_PYPROJECT" != "$VER_INIT" ]; then
    ERRORS="${ERRORS}\n  ⚠️  pyproject.toml=$VER_PYPROJECT vs __init__.py=$VER_INIT"
fi

if [ -n "$VER_README" ] && [ "$VER_PYPROJECT" != "$VER_README" ]; then
    ERRORS="${ERRORS}\n  ⚠️  pyproject.toml=$VER_PYPROJECT vs README.md=$VER_README"
fi

if [ -n "$ERRORS" ]; then
    echo "⚠️ VERSION MISMATCH DETECTED:"
    echo -e "$ERRORS"
    echo ""
    echo "Файлы для синхронизации:"
    echo "  - pyproject.toml: version = \"$VER_PYPROJECT\""
    echo "  - moleculerpy/__init__.py: __version__ = \"$VER_INIT\""
    echo "  - README.md: Current status (v$VER_README)"
    echo ""
    echo "Исправьте версии перед push/PR."
    # Warning, не блокирует (exit 0). Для блокировки: exit 2
    exit 0
fi
