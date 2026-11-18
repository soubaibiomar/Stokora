param([ValidateSet('build','up','status','generate','test','pipeline','recover','forecast','logs','stop','restart')][string]$Action='status')
$ErrorActionPreference='Stop'
$file=Join-Path $PSScriptRoot '../infra/compose/local.yml'
$commands=@{
    build=@('build')
    up=@('up','-d','--wait','--wait-timeout','300')
    status=@('ps')
    generate=@('exec','-T','postgres','stokora','generate')
    test=@('exec','-T','spark','stokora','test')
    pipeline=@('exec','-T','spark','stokora','pipeline')
    recover=@('exec','-T','spark','stokora','recover')
    forecast=@('exec','-T','spark','stokora','forecast')
    logs=@('logs','--tail','100')
    stop=@('stop')
    restart=@('stop')
}
& docker compose -p stokora-local -f $file @($commands[$Action])
if($LASTEXITCODE){throw "Docker action $Action failed with exit code $LASTEXITCODE"}
if($Action -eq 'restart'){
    & docker compose -p stokora-local -f $file @($commands.up)
    if($LASTEXITCODE){throw "Docker restart failed with exit code $LASTEXITCODE"}
}
