$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$target = Join-Path $root 'data/local/spark-jars'
New-Item -ItemType Directory -Force -Path $target | Out-Null
$artifacts = @{
    'delta-spark_2.13' = 'A1E76860B19FE1E6B205A4EA7A9099A2F9575D39BB71D49B3E679B299D0EFB91'
    'delta-storage' = 'ECC42C5AAA51C423312D9E4E48AA4A12B34740BF2C37DE0E8F279277EC3D12E4'
}
foreach ($artifact in $artifacts.Keys) {
    $file = "$artifact-4.0.1.jar"
    $path = Join-Path $target $file
    if (-not (Test-Path -LiteralPath $path)) {
        $temp = New-TemporaryFile
        try {
            curl.exe -fsSL --max-time 90 "https://repo.maven.apache.org/maven2/io/delta/$artifact/4.0.1/$file" -o $temp.FullName
            if ($LASTEXITCODE -ne 0 -or (Get-FileHash -LiteralPath $temp.FullName).Hash -ne $artifacts[$artifact]) {
                throw "Unable to verify $file"
            }
            Move-Item -LiteralPath $temp.FullName -Destination $path
        } finally {
            if (Test-Path -LiteralPath $temp.FullName) { Remove-Item -LiteralPath $temp.FullName }
        }
    }
    if ((Get-FileHash -LiteralPath $path).Hash -ne $artifacts[$artifact]) { throw "Wrong SHA-256 for $file" }
    "$file SHA256 $($artifacts[$artifact])"
}
