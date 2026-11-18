param([switch]$OutageOnly, [string]$StreamProject = 'stokora-streaming-bound')
$ErrorActionPreference = 'Stop'
$prefix = @('compose','-p',$StreamProject,'-f','infra/compose/streaming.yml','--profile','streaming','run','--rm','-T','--name','stokora-stream-spark')
function Confirm-Stream([int]$expected, [string[]]$arguments) {
    "COMMAND docker $($prefix -join ' ') $($arguments -join ' ')"
    docker @prefix @arguments 2>&1
    $code = $LASTEXITCODE
    "EXIT $code"
    if ($code -ne $expected) { throw "Unexpected stream exit $code expected $expected" }
}
function Confirm-Stage([string]$stage) {
    Confirm-Stream 0 @('--entrypoint','python','spark','-u','/workspace/stream-tests/check_stream.py',$stage)
}
if (-not $OutageOnly) {
    Confirm-Stage 'crash'
    Confirm-Stream 0 @('spark','--once')
    Confirm-Stage 'recovered'
    Confirm-Stream 0 @('spark','--once','--replay')
    Confirm-Stage 'replay'
}

$fixture='stokora-stream-fixture'
$env:CORE_SECRET_DIR=(Resolve-Path "config/secrets/$fixture").Path.Replace('\','/')
$env:POSTGRES_PASSWORD_FILE="$env:CORE_SECRET_DIR/postgres_password"
$core=@('compose','-p',$fixture,'--env-file','config/.env.example','-f','infra/compose/compose.yaml','-f','infra/compose/core.yml','--profile','core')
"COMMAND docker $($core -join ' ') stop kafka"
docker @core stop kafka
if ($LASTEXITCODE) { throw 'Kafka stop failed' }
try {
    "COMMAND docker $($prefix -join ' ') spark --once"
    $clock=[Diagnostics.Stopwatch]::StartNew()
    $output=docker @prefix spark --once 2>&1
    $code=$LASTEXITCODE
    $output
    "EXIT $code WALL_SECONDS $($clock.Elapsed.TotalSeconds)"
    if ($code -eq 0 -or -not ($output -match 'cdc_source_lookup_failed')) {
        throw 'Expected explicit Kafka unavailability'
    }
} finally {
    "COMMAND docker $($core -join ' ') up -d --wait --wait-timeout 100 kafka"
    docker @core up -d --wait --wait-timeout 100 kafka
    if ($LASTEXITCODE) { throw 'Kafka recovery failed' }
}
'COMMAND recheck connector and insert a synthetic sale via PostgreSQL'
@'
import time
from scripts.setup_stream_fixture import PROJECT
from scripts.smoke_cdc import Core, configure, insert, require
configure(PROJECT)
core = Core(PROJECT)
core.require_ready()
core.postgres(insert("after-outage"))
for _ in range(15):
    if core.end_offset() == 11:
        break
    time.sleep(2)
else:
    raise RuntimeError("no CDC after Kafka outage")
print("PASS Kafka watermark=11 after restart")
'@ | python -u -
if ($LASTEXITCODE) { throw 'Fixture did not produce a new event' }
Confirm-Stream 0 @('spark','--once')
Confirm-Stage 'outage-recovered'
