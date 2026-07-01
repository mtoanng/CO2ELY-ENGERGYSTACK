@echo off
setlocal
set "HTTP_PROXY=http://rb-proxy-apac.bosch.com:8080"
set "HTTPS_PROXY=http://rb-proxy-apac.bosch.com:8080"
set "http_proxy="
set "https_proxy="
start "" "C:\Program Files\Microsoft VS Code\Code.exe" "c:\Users\got4hc\CO2ELY_ENERGYSTACK\CO2ELY_ENERGYSTACK\bosch_co2ely_adb_batch"
