# BoundFlow demo — a costly version, auto-reverted by the control plane.
#
# Renders a reproducible terminal GIF with VHS (https://github.com/charmbracelet/vhs).
# Deterministic mock LLM — no ANTHROPIC_API_KEY needed, so the numbers (and the GIF)
# are identical every render. The rollback numbers come from the real policy audit.
#
# Generate:
#   brew install vhs
#   # 1) backend up:
#   docker compose -f ../docker-compose.dist.yml up -d
#   # 2) in THIS shell: an env with the SDK installed + a provisioned key
#   conda activate boundflow           # or: source .../bfclean/bin/activate
#   export BOUNDFLOW_API_KEY=<paste a provisioned key>
#   # 3) render:
#   vhs cost_rollback.tape             # -> writes cost_rollback.gif

Output cost_rollback.gif

Set Shell "bash"
Set FontSize 20
Set Width 1240
Set Height 760
Set Padding 24
Set Theme "Catppuccin Mocha"
Set TypingSpeed 55ms

# Start from a clean screen without recording the setup.
Hide
Type "clear" Enter
Show

Sleep 600ms
Type "# v2 shipped a pricey regression — watch BoundFlow notice and roll it back:"
Sleep 700ms
Enter
Type "python -m boundflow.examples.cost_rollback"
Sleep 500ms
Enter

# Runs 4 governed invocations (~15-20s). Wait for the final line, then hold the frame.
# Bump the timeout if your machine is slow; Sleep is the robust fallback if your VHS
# version predates `Wait`.
Wait@45s /auto-reverted a costly deploy/
Sleep 2500ms
