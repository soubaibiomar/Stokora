param([ValidateSet('inspect','start','stop','run')][string]$Action='inspect',[string]$Project='stokora-local')
if($Project -notin 'stokora-local','stokora'){throw 'Use the archived checkout for independent historical qualification.'}
if($Action -eq 'stop'){return}
$actions=@{inspect='status';start='up';run='forecast'}
& "$PSScriptRoot/stokora.ps1" $actions[$Action]
