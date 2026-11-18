$ErrorActionPreference='Stop'
$file=Join-Path $PSScriptRoot '../infra/compose/local.yml'
function Get-Containers {
    $ids=@(docker compose -p stokora-local -f $file ps -q)
    if($LASTEXITCODE -or $ids.Count -ne 4){throw 'Expected exactly four Compose services'}
    return ($ids | Sort-Object) -join ','
}
$before=Get-Containers
& "$PSScriptRoot/stokora.ps1" up
if((Get-Containers) -ne $before){throw 'Repeated startup changed the running containers'}
& "$PSScriptRoot/stokora.ps1" restart
if((Get-Containers) -ne $before){throw 'Restart changed the running containers'}
& "$PSScriptRoot/stokora.ps1" generate
& "$PSScriptRoot/stokora.ps1" pipeline
Write-Output 'PASS one stack, four stable container IDs, persistent seed and replay'
