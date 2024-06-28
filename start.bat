@echo off
setlocal

:: Start Master node
echo Starting Master node...
start "Master Node" cmd /k "go run ./master/main.go"

:: Wait for the Master node to start
timeout /t 5

:: Start Storage Server 1
echo Starting Storage Server1 ...
start "Storage Server 1" cmd /k "go run ./server/main.go --config=config.xml --server=1"

:: Start Storage Server 2
echo Starting Storage Server2 ...
start "Storage Server 2" cmd /k "go run ./server/main.go --config=config.xml --server=2"

:: Start Storage Server 3
echo Starting Storage Server3 ...
start "Storage Server 3" cmd /k "go run ./server/main.go --config=config.xml --server=3"


:: Wait for the Storage Servers to start
timeout /t 5

:: Start Client
echo Starting Client...
start "Client" cmd /k "go run client/main.go"

echo All processes started. Press any key to exit this script.
pause

endlocal
