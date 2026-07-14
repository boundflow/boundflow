# BoundFlow demo — a costly regression, auto-rolled-back, driven from the CLI.
#
# Render with VHS (https://github.com/charmbracelet/vhs):
#   # backend up + a worker registering the `checkout` handlers must be running,
#   # and a tenant id in scratchpad/.tenantid (see cli_demo_worker.py)
#   export BOUNDFLOW_API_KEY=<provisioned key>
#   vhs demo/cli_demo.tape        # -> demo/cli_demo.gif

Output demo/cli_demo.gif

Set Shell "bash"
Set FontSize 20
Set Width 1180
Set Height 820
Set Padding 22
Set Margin 18
Set MarginFill "#181825"
Set Theme "Catppuccin Mocha"
Set WindowBar Colorful
Set BorderRadius 10
Set TypingSpeed 45ms

# ── hidden setup: PATH to the boundflow wrapper, tenant, clean slate ──
Hide
Type `SP=/private/tmp/claude-501/-Users-arjunlama-src-convergeplane/914653e8-5c64-4618-ab61-b2484f1ae8a7/scratchpad` Enter
Type `export PATH="$SP/bin:$PATH"; cd "$SP"` Enter
Type `T=$(cat "$SP/.tenantid")` Enter
Type `for id in $(boundflow --json workflow list | python -c "import sys,json;[print(w['id']) for w in json.load(sys.stdin) if w['workflow_type']=='checkout']"); do boundflow workflow delete $id --yes >/dev/null; done` Enter
Type "clear" Enter
Show

Sleep 600ms
Type "# ship v2 — a costly regression — and put it under a cost budget:" Enter
Sleep 900ms
Type "boundflow workflow create checkout $T --version 2" Enter
Sleep 3s

Hide
Type `WF=$(boundflow --json workflow list | python -c "import sys,json;print(json.load(sys.stdin)[0][\"id\"])")` Enter
Show

Type "boundflow policy lifecycle set-workflow $WF --file rollback.json" Enter
Sleep 800ms
Type "#   ^ roll back to v1 if v2's lifetime cost crosses \$0.15" Enter
Sleep 2s
Type "boundflow workflow activate $WF" Enter
Sleep 2s
Type "boundflow workflow get $WF" Enter
Sleep 4s
Type "# exercise it — 3 runs push v2 past its budget:" Enter
Sleep 900ms
Type "for i in 1 2 3; do boundflow workflow invoke $WF >/dev/null; done; echo '3 runs dispatched'" Enter
Sleep 7s
Type "boundflow workflow get $WF" Enter
Sleep 4.5s
