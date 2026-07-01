$ErrorActionPreference = 'Stop'

$workspacePath = 'c:\Users\got4hc\CO2ELY_ENERGYSTACK\CO2ELY_ENERGYSTACK'
$codeExe = 'C:\Program Files\Microsoft VS Code\Code.exe'
$proxyUrl = 'http://rb-proxy-apac.bosch.com:8080'

if (-not (Test-Path $codeExe)) {
    throw "VS Code executable not found at: $codeExe"
}

$env:HTTP_PROXY = $proxyUrl
$env:HTTPS_PROXY = $proxyUrl
Remove-Item Env:http_proxy -ErrorAction SilentlyContinue
Remove-Item Env:https_proxy -ErrorAction SilentlyContinue

Start-Process -FilePath $codeExe -ArgumentList @($workspacePath)
