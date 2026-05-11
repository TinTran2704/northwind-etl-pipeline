@echo off
setlocal EnableDelayedExpansion
chcp 65001 >nul 2>&1

:: ============================================================
::  ETL Data Warehouse — Windows Control Panel
::  Yêu cầu: Docker Desktop đang chạy, Python 3.11+ trong PATH
:: ============================================================

set "PROJECT_DIR=%~dp0"
set "COMPOSE_FILE=%PROJECT_DIR%docker-compose.yml"
set "VENV_DIR=%PROJECT_DIR%.venv"
set "PYTHON_CMD=python"

:: Màu sắc (dùng ANSI nếu Windows Terminal / PowerShell mới)
set "C_RESET=[0m"
set "C_GREEN=[92m"
set "C_YELLOW=[93m"
set "C_RED=[91m"
set "C_CYAN=[96m"
set "C_BOLD=[1m"

:MAIN_MENU
cls
echo.
echo  %C_CYAN%╔══════════════════════════════════════════════════════╗%C_RESET%
echo  %C_CYAN%║   ETL Data Warehouse — Kimball Methodology           ║%C_RESET%
echo  %C_CYAN%║   Stack: PostgreSQL · pgAdmin · Apache Airflow       ║%C_RESET%
echo  %C_CYAN%╚══════════════════════════════════════════════════════╝%C_RESET%
echo.
echo  %C_BOLD%[ Docker Stack ]%C_RESET%
echo    1  ^| Start toàn bộ stack (lần đầu hoặc sau khi stop)
echo    2  ^| Stop stack (giữ data)
echo    3  ^| Restart stack
echo    4  ^| Xem trạng thái containers
echo    5  ^| Xem logs realtime
echo.
echo  %C_BOLD%[ Python Environment ]%C_RESET%
echo    6  ^| Tạo venv + cài requirements.txt
echo    7  ^| Kích hoạt venv (mở shell mới)
echo.
echo  %C_BOLD%[ ETL Pipeline ]%C_RESET%
echo    8  ^| Tải seed data (Northwind CSV + Countries JSON)
echo    9  ^| Chạy full pipeline (E-C-C-D)
echo    10 ^| Chạy chỉ Extract
echo    11 ^| Chạy chỉ Clean
echo    12 ^| Verify nguồn dữ liệu (kiểm tra URLs)
echo.
echo  %C_BOLD%[ Database ]%C_RESET%
echo    13 ^| Reset database (xóa + tạo lại schema)
echo    14 ^| Backup database → data/backups/
echo.
echo  %C_BOLD%[ Links ]%C_RESET%
echo    15 ^| Mở pgAdmin    ^(http://localhost:5050^)
echo    16 ^| Mở Airflow UI ^(http://localhost:8080^)
echo.
echo    0  ^| Thoát
echo.
set /p "CHOICE=  Chọn: "

if "%CHOICE%"=="1"  goto START_STACK
if "%CHOICE%"=="2"  goto STOP_STACK
if "%CHOICE%"=="3"  goto RESTART_STACK
if "%CHOICE%"=="4"  goto STATUS
if "%CHOICE%"=="5"  goto LOGS
if "%CHOICE%"=="6"  goto SETUP_VENV
if "%CHOICE%"=="7"  goto ACTIVATE_VENV
if "%CHOICE%"=="8"  goto SEED_DATA
if "%CHOICE%"=="9"  goto RUN_PIPELINE
if "%CHOICE%"=="10" goto RUN_EXTRACT
if "%CHOICE%"=="11" goto RUN_CLEAN
if "%CHOICE%"=="12" goto VERIFY_SOURCES
if "%CHOICE%"=="13" goto RESET_DB
if "%CHOICE%"=="14" goto BACKUP_DB
if "%CHOICE%"=="15" goto OPEN_PGADMIN
if "%CHOICE%"=="16" goto OPEN_AIRFLOW
if "%CHOICE%"=="0"  goto EXIT
goto MAIN_MENU

:: ────────────────────────────────────────────────────────────
:START_STACK
cls
call :PRINT_HEADER "Starting ETL Stack"

:: Kiểm tra Docker Desktop
call :CHECK_DOCKER
if errorlevel 1 goto MAIN_MENU

:: Kiểm tra file .env
if not exist "%PROJECT_DIR%.env" (
    echo  %C_YELLOW%[WARN] .env không tồn tại → copy từ .env.example%C_RESET%
    copy "%PROJECT_DIR%.env.example" "%PROJECT_DIR%.env" >nul
    echo  %C_GREEN%[OK] Đã tạo .env từ .env.example%C_RESET%
)

:: Tạo thư mục cần thiết
echo  Tạo thư mục data...
for %%d in (data\raw data\staging data\error data\seed data\backups logs\airflow dags plugins src config) do (
    if not exist "%PROJECT_DIR%%%d" (
        mkdir "%PROJECT_DIR%%%d" 2>nul
        echo     + %%d
    )
)

echo.
echo  %C_YELLOW%Pulling images nếu chưa có (lần đầu có thể mất 3-10 phút)...%C_RESET%
docker compose -f "%COMPOSE_FILE%" pull --quiet

echo.
echo  %C_YELLOW%Khởi động PostgreSQL trước...%C_RESET%
docker compose -f "%COMPOSE_FILE%" up -d postgres

echo  Đợi PostgreSQL sẵn sàng (tối đa 60s)...
call :WAIT_FOR_HEALTHY etl_postgres 60
if errorlevel 1 (
    echo  %C_RED%[ERROR] PostgreSQL không khởi động được. Kiểm tra logs:%C_RESET%
    echo    docker logs etl_postgres
    pause
    goto MAIN_MENU
)

echo.
echo  %C_YELLOW%Khởi động pgAdmin...%C_RESET%
docker compose -f "%COMPOSE_FILE%" up -d pgadmin

echo.
echo  %C_YELLOW%Chạy Airflow init (migrate DB + tạo admin user)...%C_RESET%
docker compose -f "%COMPOSE_FILE%" up airflow-init
echo  %C_GREEN%[OK] Airflow init hoàn thành%C_RESET%

echo.
echo  %C_YELLOW%Khởi động Airflow webserver + scheduler...%C_RESET%
docker compose -f "%COMPOSE_FILE%" up -d airflow-webserver airflow-scheduler

echo.
echo  Đợi Airflow Webserver sẵn sàng (tối đa 90s)...
call :WAIT_URL_READY "http://localhost:8080/health" 90
if errorlevel 1 (
    echo  %C_YELLOW%[WARN] Airflow có thể chưa ready — kiểm tra logs: docker logs etl_airflow_web%C_RESET%
) else (
    echo  %C_GREEN%[OK] Airflow sẵn sàng%C_RESET%
)

echo.
echo  %C_GREEN%╔══════════════════════════════════════════════════════╗%C_RESET%
echo  %C_GREEN%║  Stack đã khởi động!                                 ║%C_RESET%
echo  %C_GREEN%║                                                      ║%C_RESET%
echo  %C_GREEN%║  pgAdmin  : http://localhost:5050                    ║%C_RESET%
echo  %C_GREEN%║  Airflow  : http://localhost:8080                    ║%C_RESET%
echo  %C_GREEN%║                                                      ║%C_RESET%
echo  %C_GREEN%║  admin / admin (cả hai UI)                           ║%C_RESET%
echo  %C_GREEN%╚══════════════════════════════════════════════════════╝%C_RESET%
echo.
pause
goto MAIN_MENU

:: ────────────────────────────────────────────────────────────
:STOP_STACK
call :PRINT_HEADER "Stopping ETL Stack"
docker compose -f "%COMPOSE_FILE%" stop
echo  %C_GREEN%[OK] Stack đã dừng. Data được giữ nguyên.%C_RESET%
echo  Dùng lựa chọn 1 để khởi động lại.
pause
goto MAIN_MENU

:: ────────────────────────────────────────────────────────────
:RESTART_STACK
call :PRINT_HEADER "Restarting ETL Stack"
docker compose -f "%COMPOSE_FILE%" restart
echo  %C_GREEN%[OK] Restart hoàn thành%C_RESET%
pause
goto MAIN_MENU

:: ────────────────────────────────────────────────────────────
:STATUS
call :PRINT_HEADER "Container Status"
docker compose -f "%COMPOSE_FILE%" ps
echo.
echo  %C_CYAN%Ports:%C_RESET%
echo    PostgreSQL : localhost:5432
echo    pgAdmin    : http://localhost:5050
echo    Airflow    : http://localhost:8080
echo.
pause
goto MAIN_MENU

:: ────────────────────────────────────────────────────────────
:LOGS
call :PRINT_HEADER "Logs (Ctrl+C để dừng)"
echo  Services: postgres / pgadmin / airflow-webserver / airflow-scheduler
echo.
set /p "SVC=  Xem log service nào? (Enter = all): "
if "%SVC%"=="" (
    docker compose -f "%COMPOSE_FILE%" logs -f --tail=50
) else (
    docker compose -f "%COMPOSE_FILE%" logs -f --tail=50 %SVC%
)
goto MAIN_MENU

:: ────────────────────────────────────────────────────────────
:SETUP_VENV
call :PRINT_HEADER "Setup Python Virtual Environment"

:: Kiểm tra Python
%PYTHON_CMD% --version >nul 2>&1
if errorlevel 1 (
    echo  %C_RED%[ERROR] Python không tìm thấy trong PATH.%C_RESET%
    echo  Cài Python 3.11+ từ https://python.org/downloads/
    pause
    goto MAIN_MENU
)

for /f "tokens=*" %%v in ('%PYTHON_CMD% --version 2^>^&1') do echo  Python: %%v

if exist "%VENV_DIR%" (
    echo  %C_YELLOW%[WARN] .venv đã tồn tại. Tạo lại? (y/N)%C_RESET%
    set /p "CONFIRM="
    if /i "!CONFIRM!"=="y" (
        rmdir /s /q "%VENV_DIR%"
    ) else (
        goto MAIN_MENU
    )
)

echo  Tạo virtual environment...
%PYTHON_CMD% -m venv "%VENV_DIR%"

echo  Cài đặt dependencies...
"%VENV_DIR%\Scripts\pip.exe" install --upgrade pip --quiet
"%VENV_DIR%\Scripts\pip.exe" install -r "%PROJECT_DIR%requirements.txt" --quiet

if errorlevel 1 (
    echo  %C_RED%[ERROR] Cài đặt thất bại. Kiểm tra requirements.txt%C_RESET%
) else (
    echo  %C_GREEN%[OK] Virtual environment sẵn sàng tại .venv\%C_RESET%
    echo  Dùng lựa chọn 7 để kích hoạt.
)
pause
goto MAIN_MENU

:: ────────────────────────────────────────────────────────────
:ACTIVATE_VENV
call :PRINT_HEADER "Activate Python venv"
if not exist "%VENV_DIR%\Scripts\activate.bat" (
    echo  %C_RED%[ERROR] .venv không tồn tại. Chạy lựa chọn 6 trước.%C_RESET%
    pause
    goto MAIN_MENU
)
echo  Mở cmd shell mới với venv đã kích hoạt...
echo  Gõ "exit" để trở về.
cmd /k ""%VENV_DIR%\Scripts\activate.bat" && echo [OK] venv activated && echo PYTHONPATH=%PROJECT_DIR% && set PYTHONPATH=%PROJECT_DIR%"
goto MAIN_MENU

:: ────────────────────────────────────────────────────────────
:SEED_DATA
call :PRINT_HEADER "Download Seed Data"
echo  Tải Northwind CSV + Countries JSON vào data\seed\
echo.

if not exist "%PROJECT_DIR%data\seed\northwind" mkdir "%PROJECT_DIR%data\seed\northwind"
if not exist "%PROJECT_DIR%data\seed\countries" mkdir "%PROJECT_DIR%data\seed\countries"

:: Dùng Python để tải (tránh phụ thuộc curl trên Windows cũ)
if not exist "%VENV_DIR%\Scripts\python.exe" (
    echo  %C_YELLOW%[WARN] Venv chưa có → dùng system Python%C_RESET%
    set "PY_CMD=%PYTHON_CMD%"
) else (
    set "PY_CMD=%VENV_DIR%\Scripts\python.exe"
)

"%PY_CMD%" "%PROJECT_DIR%scripts\download_seed.py"

if errorlevel 1 (
    echo  %C_RED%[ERROR] Tải thất bại. Kiểm tra kết nối mạng.%C_RESET%
) else (
    echo  %C_GREEN%[OK] Seed data đã sẵn sàng trong data\seed\%C_RESET%
)
pause
goto MAIN_MENU

:: ────────────────────────────────────────────────────────────
:RUN_PIPELINE
call :PRINT_HEADER "Run Full ETL Pipeline (E-C-C-D)"
call :CHECK_VENV
if errorlevel 1 goto MAIN_MENU
echo  %C_YELLOW%Chạy pipeline...%C_RESET%
"%VENV_DIR%\Scripts\python.exe" -m src.orchestration.pipeline run
if errorlevel 1 (
    echo  %C_RED%[ERROR] Pipeline thất bại. Xem logs ở trên.%C_RESET%
) else (
    echo  %C_GREEN%[OK] Pipeline hoàn thành!%C_RESET%
)
pause
goto MAIN_MENU

:: ────────────────────────────────────────────────────────────
:RUN_EXTRACT
call :PRINT_HEADER "Run Extract Phase Only"
call :CHECK_VENV
if errorlevel 1 goto MAIN_MENU
"%VENV_DIR%\Scripts\python.exe" -m src.orchestration.pipeline run --phase extract
pause
goto MAIN_MENU

:: ────────────────────────────────────────────────────────────
:RUN_CLEAN
call :PRINT_HEADER "Run Clean Phase Only"
call :CHECK_VENV
if errorlevel 1 goto MAIN_MENU
"%VENV_DIR%\Scripts\python.exe" -m src.orchestration.pipeline run --phase clean
pause
goto MAIN_MENU

:: ────────────────────────────────────────────────────────────
:VERIFY_SOURCES
call :PRINT_HEADER "Verify Data Sources"
call :CHECK_VENV
if errorlevel 1 goto MAIN_MENU
"%VENV_DIR%\Scripts\python.exe" "%PROJECT_DIR%scripts\verify_sources.py"
pause
goto MAIN_MENU

:: ────────────────────────────────────────────────────────────
:RESET_DB
call :PRINT_HEADER "Reset Database"
echo  %C_RED%[CẢNH BÁO] Toàn bộ dữ liệu warehouse sẽ bị xóa!%C_RESET%
echo  Postgres volume sẽ bị xóa và tạo lại từ đầu.
echo.
set /p "CONFIRM=  Gõ YES để xác nhận: "
if not "%CONFIRM%"=="YES" (
    echo  Hủy.
    pause
    goto MAIN_MENU
)
docker compose -f "%COMPOSE_FILE%" down -v
docker compose -f "%COMPOSE_FILE%" up -d postgres
call :WAIT_FOR_HEALTHY etl_postgres 60
echo  %C_GREEN%[OK] Database đã reset. Chạy lựa chọn 1 để khởi động lại stack.%C_RESET%
pause
goto MAIN_MENU

:: ────────────────────────────────────────────────────────────
:BACKUP_DB
call :PRINT_HEADER "Backup Database"
call :CHECK_DOCKER
if errorlevel 1 goto MAIN_MENU

for /f "tokens=2 delims==" %%d in ('wmic os get LocalDateTime /value') do set "DT=%%d"
set "TIMESTAMP=%DT:~0,8%-%DT:~8,6%"
set "BACKUP_FILE=%PROJECT_DIR%data\backups\northwind_dw_%TIMESTAMP%.sql"

if not exist "%PROJECT_DIR%data\backups" mkdir "%PROJECT_DIR%data\backups"

echo  Đang backup northwind_dw...
docker exec etl_postgres pg_dump -U etl_user -d northwind_dw > "%BACKUP_FILE%"

if errorlevel 1 (
    echo  %C_RED%[ERROR] Backup thất bại%C_RESET%
) else (
    echo  %C_GREEN%[OK] Backup xong: %BACKUP_FILE%%C_RESET%
)
pause
goto MAIN_MENU

:: ────────────────────────────────────────────────────────────
:OPEN_PGADMIN
start http://localhost:5050
echo  Mở http://localhost:5050 — đăng nhập: admin@etl.local / admin
pause
goto MAIN_MENU

:OPEN_AIRFLOW
start http://localhost:8080
echo  Mở http://localhost:8080 — đăng nhập: admin / admin
pause
goto MAIN_MENU

:: ────────────────────────────────────────────────────────────
:EXIT
echo.
echo  %C_CYAN%Stack vẫn đang chạy nền. Dùng "docker compose stop" để dừng.%C_RESET%
echo.
endlocal
exit /b 0

:: ============================================================
::  HELPER FUNCTIONS
:: ============================================================

:PRINT_HEADER
cls
echo.
echo  %C_CYAN%══════════════════════════════════════%C_RESET%
echo  %C_BOLD%  %~1%C_RESET%
echo  %C_CYAN%══════════════════════════════════════%C_RESET%
echo.
exit /b 0

:CHECK_DOCKER
docker info >nul 2>&1
if errorlevel 1 (
    echo  %C_RED%[ERROR] Docker Desktop không chạy hoặc chưa cài.%C_RESET%
    echo  Khởi động Docker Desktop và thử lại.
    pause
    exit /b 1
)
exit /b 0

:CHECK_VENV
if not exist "%VENV_DIR%\Scripts\python.exe" (
    echo  %C_RED%[ERROR] Python venv chưa tạo. Chạy lựa chọn 6 trước.%C_RESET%
    pause
    exit /b 1
)
exit /b 0

:WAIT_FOR_HEALTHY
:: %1 = container name, %2 = max seconds
set "CONTAINER=%~1"
set "MAX_WAIT=%~2"
set "WAITED=0"
:HEALTH_LOOP
docker inspect --format "{{.State.Health.Status}}" %CONTAINER% 2>nul | findstr "healthy" >nul
if not errorlevel 1 (
    echo  %C_GREEN%[OK] %CONTAINER% healthy%C_RESET%
    exit /b 0
)
if %WAITED% GEQ %MAX_WAIT% (
    echo  %C_RED%[TIMEOUT] %CONTAINER% chưa healthy sau %MAX_WAIT%s%C_RESET%
    exit /b 1
)
timeout /t 3 /nobreak >nul
set /a WAITED+=3
echo  ... đợi %CONTAINER% (%WAITED%s/%MAX_WAIT%s)
goto HEALTH_LOOP

:WAIT_URL_READY
:: %1 = URL, %2 = max seconds
set "URL=%~1"
set "MAX_WAIT=%~2"
set "WAITED=0"
:URL_LOOP
curl -sf "%URL%" >nul 2>&1
if not errorlevel 1 (
    exit /b 0
)
if %WAITED% GEQ %MAX_WAIT% (
    exit /b 1
)
timeout /t 5 /nobreak >nul
set /a WAITED+=5
echo  ... đợi %URL% (%WAITED%s/%MAX_WAIT%s)
goto URL_LOOP