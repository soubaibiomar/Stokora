param([switch]$OutageOnly)
if($OutageOnly){throw 'Use the archived checkout and restore plan for the isolated outage rehearsal.'}
& "$PSScriptRoot/stokora.ps1" recover
