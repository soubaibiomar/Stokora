param(
    [ValidateSet('start','stop','run','inspect')][string]$Action='inspect',
    [ValidateSet('stokora-test','stokora-test-review')][string]$Project='stokora-test'
)
$ErrorActionPreference='Stop'
Set-Location (Join-Path $PSScriptRoot '..')
$image='stokora-sec01-spark-runtime@sha256:d30d4898d6fe005158ee0b6bc2f822020820018a0c3f787eac15332adf9bbdb8'
$env:STOKORA_SEC01_SPARK_IMAGE=$image
$name="$Project-forecast"
$compose=@('compose','-p',$Project,'-f','infra/compose/forecast.yml','-f','docs/evidence/sec-01-staging-and-spark-closure/spark-runtime.yml','-f','infra/compose/forecast-test.yml')
function Invoke-Docker([string[]]$Arguments) {
    $result=& docker @Arguments
    if($LASTEXITCODE){throw "Docker operation $($Arguments[0]) exited $LASTEXITCODE"}
    $result
}
$bind=@{'/workspace/pipelines'='pipelines';'/workspace/contracts'='contracts';'/workspace/tests/ml'='tests/ml';'/opt/bronze-jars'='data/local/spark-jars'}
    $vol=@{'/gold'='stokora-step06-baseline_derived';'/opt/gx'='stokora-step06_gx19'}
function Stop-Worker($id){
    Invoke-Docker @('stop',$id) | Out-Null
    $limit=[datetime]::UtcNow.AddSeconds(15)
    while(Invoke-Docker @('ps','-aq','--filter',"id=$id")){
        if([datetime]::UtcNow -gt $limit){throw 'Docker did not finish automatic removal'}
        Start-Sleep -Milliseconds 150
    }
}
function Get-Worker {
    $ids=@(Invoke-Docker @('ps','-aq','--no-trunc','--filter',"name=^/${name}$"))
    if(!$ids.Count){return}
    $c=(Invoke-Docker @('inspect',$ids[0]) | ConvertFrom-Json)[0]
    if($c.Config.Labels.'com.docker.compose.project' -ne $Project -or
       $c.Config.Labels.'org.stokora.lifecycle' -ne 'disposable-step07' -or
       $c.Config.Labels.'com.docker.compose.service' -ne 'spark' -or
       $c.Image -ne $image.Split('@')[1] -or !$c.HostConfig.AutoRemove -or
       !$c.HostConfig.ReadonlyRootfs -or $c.HostConfig.NetworkMode -ne 'none' -or $c.HostConfig.Privileged -or
       @($c.Mounts | Where-Object {$_.RW -and ($_.Type -ne 'tmpfs' -or $_.Destination -notin '/home/jovyan/work','/tmp','/native')}).Count){
        throw 'Worker identity or isolation mismatch'
    }
    if($c.HostConfig.CapDrop -notcontains 'ALL' -or $c.HostConfig.CapAdd -or $c.HostConfig.PidMode -or $c.HostConfig.Devices -or
       $c.HostConfig.NanoCpus -ne 2000000000 -or $c.HostConfig.Memory -ne 3221225472 -or
       !($c.HostConfig.SecurityOpt -match 'no-new-privileges')){throw 'Runtime limits or isolation changed'}
    $inputs=@($c.Mounts | Where-Object {!$_.RW})
    if($inputs.Count -ne 6){throw 'Unexpected number of read-only inputs'}
    foreach($m in $inputs){
        if($m.Type -eq 'volume' -and $vol.ContainsKey($m.Destination) -and $vol[$m.Destination] -eq $m.Name){continue}
        if($m.Type -eq 'bind' -and $bind.ContainsKey($m.Destination)){
            $source=$m.Source -replace '^/run/desktop/mnt/host/([a-z])/', '$1:/'
            if([IO.Path]::GetFullPath($source) -eq (Resolve-Path $bind[$m.Destination]).Path){continue}
        }
        throw 'Unexpected read-only attachment'
    }
    return $c
}
$mutex=[Threading.Mutex]::new($false,"Local\STOKORA-$Project-lifecycle")
$held=$false
try {
    try {$held=$mutex.WaitOne(0)} catch [Threading.AbandonedMutexException] {$held=$true}
    if(!$held){throw 'Another lifecycle operation owns this slot'}
    $worker=Get-Worker
    if($Action -eq 'inspect'){if($worker){$worker.Id}else{'absent'};return}
    if($Action -eq 'stop'){if($worker){Stop-Worker $worker.Id};return}
    if($worker){
        if($Action -eq 'run' -or !$worker.State.Running){throw 'Slot occupied; use the other slot or stop this fixture first'}
        $worker.Id
        return
    }
    $config=Invoke-Docker ($compose+@('config','--format','json')) | ConvertFrom-Json
    $s=$config.services.spark
    if($s.image -ne $image -or $s.network_mode -ne 'none' -or !$s.read_only -or $s.ports -or $s.secrets -or $s.privileged -or $s.cap_add -or $s.cap_drop -notcontains 'ALL' -or $s.volumes.Count -ne 7){throw 'Unexpected fixture configuration'}
    foreach($m in $s.volumes){
        if($m.type -eq 'tmpfs' -and $m.target -eq '/home/jovyan/work'){continue}
        if(!$m.read_only){throw 'Writable attachment refused'}
        if($m.type -eq 'bind' -and $bind.ContainsKey($m.target) -and (Resolve-Path -LiteralPath $m.source).Path -eq (Resolve-Path -LiteralPath $bind[$m.target]).Path){continue}
        if($m.type -eq 'volume' -and $vol.ContainsKey($m.target) -and $config.volumes.($m.source).external -and $config.volumes.($m.source).name -eq $vol[$m.target]){
            Invoke-Docker @('volume','inspect',$vol[$m.target],'--format','{{.Name}}') | Out-Null
            continue
        }
        throw 'Unexpected attachment refused'
    }
    $created=$null
    try {
        $created=(Invoke-Docker ($compose+@('run','--pull','never','--rm','--no-deps','-d','--name',$name,'--entrypoint','sleep','spark','3600')) | Select-Object -Last 1)
        $worker=Get-Worker
        if(!$worker -or $worker.Id -ne $created -or !$worker.State.Running){throw 'Worker startup failed'}
        if($Action -eq 'run'){Invoke-Docker @('exec',$worker.Id,'python','-Bu','/workspace/tests/ml/test_leakage.py')}
        else {$worker.Id}
    } finally {
        if($created -and ($Action -eq 'run' -or !$worker -or !$worker.State.Running)){
            $owned=Get-Worker
            if($owned -and $owned.Id -eq $created){Stop-Worker $owned.Id | Out-Null}
        }
    }
} finally {if($held){$mutex.ReleaseMutex()};$mutex.Dispose()}
