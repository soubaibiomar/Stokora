$ErrorActionPreference = 'Stop'
& (Join-Path $PSScriptRoot 'fetch_delta.ps1')
$directory = Join-Path (Split-Path -Parent $PSScriptRoot) 'data/local/kafka-jars'
New-Item -ItemType Directory -Force $directory | Out-Null
$artifacts = [ordered]@{
  'org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.0/spark-sql-kafka-0-10_2.13-4.0.0.jar' = '33A3B8B04DF4A2E43C9D5BF8CA5972936A1AC662F2714E014D00EF6F27468BF3'
  'org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.0/spark-token-provider-kafka-0-10_2.13-4.0.0.jar' = 'F2BE3B76F9E6CBE63379D4E4E0DC14F59DBF056AE2AD16FF15AC108004C86767'
  'org/apache/kafka/kafka-clients/3.9.2/kafka-clients-3.9.2.jar' = '014B4EF36884478A932641818B080C36A8F8E2E92DC771E800C9D2694F4F83D8'
  'org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar' = '6D3BD18DF8410F3E31B031ACA582CC109342358A62A2759EBD0C4CDF30D06F8B'
}
foreach ($relative in $artifacts.Keys) {
  $name = Split-Path -Leaf $relative
  $path = Join-Path $directory $name
  if (-not (Test-Path -LiteralPath $path)) {
    $temp = New-TemporaryFile
    try {
      curl.exe -fsSL --max-time 90 "https://repo.maven.apache.org/maven2/$relative" -o $temp.FullName
      if ($LASTEXITCODE -or (Get-FileHash $temp.FullName).Hash -ne $artifacts[$relative]) {
        throw "Unable to verify $name"
      }
      Move-Item -LiteralPath $temp.FullName -Destination $path
    } finally {
      if (Test-Path -LiteralPath $temp.FullName) { Remove-Item -LiteralPath $temp.FullName }
    }
  }
  if ((Get-FileHash -LiteralPath $path).Hash -ne $artifacts[$relative]) { throw "Wrong SHA-256 for $name" }
  "$name SHA256 $($artifacts[$relative])"
}
