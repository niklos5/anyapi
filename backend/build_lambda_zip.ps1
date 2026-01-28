Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot

if (Test-Path build) {
  Remove-Item -Recurse -Force build
}

New-Item -ItemType Directory -Force build | Out-Null

python -m pip install -r requirements.txt -t build

Copy-Item *.py -Destination build
Copy-Item -Recurse lambdas -Destination build\lambdas

Compress-Archive -Path build\* -DestinationPath lambda.zip -Force
Write-Host "Created backend\lambda.zip"
