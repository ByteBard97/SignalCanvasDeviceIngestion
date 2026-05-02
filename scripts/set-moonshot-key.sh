#!/usr/bin/env bash
#
# Insert your Moonshot (Kimi) API key into the project's .env file.
#
# Usage:
#   1. Edit the line below and paste your key between the quotes.
#   2. Run: bash scripts/set-moonshot-key.sh
#   3. Delete this file (or revert the edit) so the key isn't sitting in source control.
#
# This script is .gitignored via *.local.sh — but DO NOT commit it with the key in it anyway.

# ──────────────────────────────────────────────────────────────────────────────
# PASTE YOUR KEY HERE (between the quotes), then save and run this script.
MOONSHOT_API_KEY=""
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

if [[ "${MOONSHOT_API_KEY}" == "REPLACE_ME_WITH_YOUR_KEY" || -z "${MOONSHOT_API_KEY}" ]]; then
    echo "ERROR: edit this script and paste your key into MOONSHOT_API_KEY first." >&2
    exit 1
fi

# Resolve the project root (parent of the directory holding this script).
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${PROJECT_ROOT}/.env"
GITIGNORE="${PROJECT_ROOT}/.gitignore"

# Refuse to run if .env is somehow not gitignored.
if ! grep -qE '^\.env$' "${GITIGNORE}" 2>/dev/null; then
    echo "ERROR: .env is not in ${GITIGNORE}. Refusing to write the key." >&2
    echo "       Add a line containing exactly '.env' to .gitignore first." >&2
    exit 1
fi

# Create .env from .env.example on first run.
if [[ ! -f "${ENV_FILE}" ]]; then
    if [[ -f "${PROJECT_ROOT}/.env.example" ]]; then
        cp "${PROJECT_ROOT}/.env.example" "${ENV_FILE}"
        echo "Created ${ENV_FILE} from .env.example"
    else
        touch "${ENV_FILE}"
        echo "Created empty ${ENV_FILE}"
    fi
fi

# Update or append MOONSHOT_API_KEY in .env.
if grep -qE '^MOONSHOT_API_KEY=' "${ENV_FILE}"; then
    # Replace existing line (portable BSD/GNU sed via temp file)
    tmp="$(mktemp)"
    awk -v key="${MOONSHOT_API_KEY}" '
        /^MOONSHOT_API_KEY=/ { print "MOONSHOT_API_KEY=" key; next }
        { print }
    ' "${ENV_FILE}" > "${tmp}"
    mv "${tmp}" "${ENV_FILE}"
    echo "Updated MOONSHOT_API_KEY in ${ENV_FILE}"
else
    printf '\nMOONSHOT_API_KEY=%s\n' "${MOONSHOT_API_KEY}" >> "${ENV_FILE}"
    echo "Appended MOONSHOT_API_KEY to ${ENV_FILE}"
fi

chmod 600 "${ENV_FILE}"

echo
echo "Done. Key length: ${#MOONSHOT_API_KEY} chars."
echo "Reminder: clear the key from this script before sharing it (or delete the script)."
