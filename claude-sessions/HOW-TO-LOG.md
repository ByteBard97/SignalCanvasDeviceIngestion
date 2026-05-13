# How to write a session log entry

You are required to write a session log entry. Do this now before continuing.

## Step 1 — Write the session file
Use the Write tool to append a paragraph to the session file shown in the stop message.
Write for a future Claude who needs to pick up this work cold: what was decided, what
was built, what's pending. Not a tool call log — the *why* and *what was decided*.

## Step 2 — Append to INDEX.md
Use this exact Bash command (replace the placeholders):
```
python3 -c "
import os
line = '[YYYY-MM-DD HH:MM] [SESSION_ID] One-line summary → YYYY-MM-DD_SESSION_ID.md\n'
fd = os.open('claude-sessions/INDEX.md', os.O_WRONLY | os.O_CREAT | os.O_APPEND)
os.write(fd, line.encode('utf-8'))
os.close(fd)
"
```

## Step 3 — Continue
After writing both files, continue the conversation normally. Do not announce that you
wrote the log — just continue.
