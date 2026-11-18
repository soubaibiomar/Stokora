param([ValidateSet('inspect','start','stop')][string]$Action='inspect',[string]$Slot='dev')
if($Slot -ne 'dev'){throw 'Use the archived checkout and restore plan for the historical staging environment.'}
$actions=@{inspect='status';start='up';stop='stop'}
& "$PSScriptRoot/stokora.ps1" $actions[$Action]
