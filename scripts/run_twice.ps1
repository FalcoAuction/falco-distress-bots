# scripts/run_twice.ps1
# Runs the FALCO engine twice back-to-back.
# Compare the two summary blocks: created should drop to 0 on run 2 (all updated),
# confirming that Lead Key deduplication is working correctly.
#
# Usage:
#   .\scripts\run_twice.ps1
#
# For dry-run (no Notion writes):
#   $env:FALCO_DRY_RUN = "1"; .\scripts\run_twice.ps1

$ErrorActionPreference = "Continue"

Write-Host ""
Write-Host "================================================================"
Write-Host " FALCO RUN 1"
Write-Host "================================================================"
py -3 -m src.run_all

Write-Host ""
Write-Host "================================================================"
Write-Host " FALCO RUN 2  (created counts should be 0 — all updated)"
Write-Host "================================================================"
py -3 -m src.run_all

Write-Host ""
Write-Host "================================================================"
Write-Host " Done. Review created= and updated= in each bot summary above."
Write-Host " If dedupe is working: run 1 shows created>0, run 2 shows created=0."
Write-Host "================================================================"
