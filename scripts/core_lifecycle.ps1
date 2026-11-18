param(
    [ValidateSet('inspect','start','stop')][string]$Action='inspect',
    [ValidateSet('dev','staging')][string]$Slot='dev'
)
$ErrorActionPreference='Stop'
Set-Location (Join-Path $PSScriptRoot '..')
$project=@{dev='stokora-security-final';staging='stokora-sec01-owner-staging'}[$Slot]
# Restore validation enrolled immutable container IDs on this workstation.
$records=@(Import-Csv docs/evidence/final-stabilization/inventory-before.csv |
    Where-Object {$_.type -eq 'container' -and $_.project -eq $project})
function Read-Docker([string[]]$Arguments){
    $data=& docker @Arguments
    if($LASTEXITCODE){throw "Docker $($Arguments[0]) exited $LASTEXITCODE"}
    $data
}
$lock=[Threading.Mutex]::new($false,"Local\STOKORA-$project-lifecycle")
$held=$false
try{
    try{$held=$lock.WaitOne(0)}catch [Threading.AbandonedMutexException]{$held=$true}
    if(!$held){throw 'Core lifecycle operation in progress'}
    $ids=@(Read-Docker @('ps','-aq','--no-trunc','--filter',"label=com.docker.compose.project=$project"))
    if($records.Count -ne 3 -or $ids.Count -ne 3 -or (Compare-Object $records.id $ids)){throw 'Enrollment differs from Docker'}
    $items=Read-Docker (@('inspect')+$ids) | ConvertFrom-Json
    $network=(Read-Docker @('network','inspect',"${project}_data") | ConvertFrom-Json)[0]
    if(!$network.Internal -or $network.Labels.'com.docker.compose.project' -ne $project -or @($network.Containers.PSObject.Properties.Name | Where-Object {$_ -and $_ -notin $ids}).Count){throw 'Network boundary changed'}
    $services=@{}
    foreach($c in $items){
        $service=$c.Config.Labels.'com.docker.compose.service'
        $record=$records | Where-Object id -eq $c.Id
        if($c.Image -ne $record.image -or $service -notin 'postgres','kafka','connect' -or
           $c.Name -ne "/$project-$service-1" -or $services.ContainsKey($service) -or $c.HostConfig.Privileged -or
           @($c.HostConfig.PortBindings.PSObject.Properties.Name | Where-Object {$_}).Count -or
           @($c.NetworkSettings.Networks.PSObject.Properties.Name).Count -ne 1 -or
           !$c.NetworkSettings.Networks.("${project}_data") -or
           $c.Config.Labels.'com.docker.compose.project.config_files' -notlike '*core-tls.yml*'){throw 'Service identity or exposure changed'}
        foreach($m in $c.Mounts | Where-Object RW){
            if($m.Type -ne 'volume'){throw 'Unexpected writable bind'}
            $owners=@(Read-Docker @('ps','-aq','--no-trunc','--filter',"volume=$($m.Name)"))
            if($owners.Count -ne 1 -or $owners[0] -ne $c.Id){throw 'Shared writable attachment'}
        }
        $services[$service]=$c
    }
    if($Action -eq 'inspect'){$items | Select-Object Name,Id,@{N='State';E={$_.State.Status}};return}
    $order=@('postgres','kafka','connect')
    if($Action -eq 'stop'){
        [array]::Reverse($order)
        foreach($service in $order){$c=$services[$service];if($c.State.Running){Read-Docker @('stop',$c.Id)}}
        return
    }
    $started=@()
    try{
        foreach($service in $order){
            $c=$services[$service]
            if(!$c.State.Running){Read-Docker @('start',$c.Id);$started+= $c.Id}
            $deadline=[datetime]::UtcNow.AddSeconds(150)
            do{
                $health=Read-Docker @('inspect','--format','{{.State.Health.Status}}',$c.Id)
                if($health -eq 'healthy'){break}
                Start-Sleep -Seconds 2
            }while([datetime]::UtcNow -lt $deadline)
            if($health -ne 'healthy'){throw "$service did not become healthy"}
        }
    }catch{
        [array]::Reverse($started)
        foreach($id in $started){Read-Docker @('stop',$id)}
        throw
    }
}finally{if($held){$lock.ReleaseMutex()};$lock.Dispose()}
