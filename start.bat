@echo off
setlocal EnableDelayedExpansion
chcp 65001 >nul 2>&1

:: ============================================================
::  ETL Stack — Windows Control Panel
::  PostgreSQL + Airflow (KHÔNG pgAdmin)
:: ============================================================

set "ROOT=%~dp0"
set "COMPOSE=%ROOT%docker-compose.yml"
set "VENV=%ROOT%.venv"

:MENU
cls
echo.
echo  ╔══════════════════════════════════════════════════════╗
echo  ║   ETL Data Warehouse — Kimball Methodology           ║
echo  ║   Stack: PostgreSQL + Apache Airflow                 ║
echo  ╚══════════════════════════════════════════════════════╝
echo.
echo   [ DOCKER ]
echo     1  Start stack (PostgreSQL + Airflow)
echo     2  Stop stack (giữ data)
echo     3  Status containers
echo     4  Logs realtime
echo     5  Reset database (xóa hết, làm lại)
echo.
echo   [ PYTHON ]
echo     6  Setup .venv + cài requirements
echo     7  Mở shell với venv kích hoạt
echo.
echo   [ DATA / ETL ]
echo     8  Tải seed data từ GitHub
echo     9  Verify nguồn dữ liệu (kiểm tra URLs)
echo     10 Chạy ETL pipeline (full)
echo.
echo   [ LINKS ]
echo     11 Mở Airflow UI ^(http://localhost:8080^)
echo.
echo     0  Thoát
echo.
set /p "C=  Chọn: "

if "%C%"=="1"  goto START
if "%C%"=="2"  goto STOP
if "%C%"=="3"  goto STATUS
if "%C%"=="4"  goto LOGS
if "%C%"=="5"  goto RESET
if "%C%"=="6"  goto VENV
if "%C%"=="7"  goto SHELL
if "%C%"=="8"  goto SEED
if "%C%"=="9"  goto VERIFY
if "%C%"=="10" goto RUN
if "%C%"=="11" goto AIRFLOW
if "%C%"=="0"  exit /b 0
goto MENU

:START
cls
echo === Starting Stack ===
echo.

:: Kiểm tra Docker
docker info >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker Desktop không chạy. Khởi động trước rồi thử lại.
    pause & goto MENU
)

:: Kiểm tra .env
if not exist "%ROOT%.env" (
    echo [WARN] .env không tồn tại. Copy từ .env.example...
    copy "%ROOT%.env.example" "%ROOT%.env" >nul
    echo [OK] Đã tạo .env. Bạn nên mở file này và sửa password trước khi production.
)

:: Tạo thư mục cần thiết
for %%d in (data\seed data\raw data\staging data\error logs dags src config) do (
    if not exist "%ROOT%%%d" mkdir "%ROOT%%%d" 2>nul
)

echo Pulling images (lần đầu có thể mất 3-10 phút)...
docker compose -f "%COMPOSE%" pull --quiet

echo.
echo Bước 1/3: Khởi động PostgreSQL...
docker compose -f "%COMPOSE%" up -d postgres

echo Đợi PostgreSQL healthy...
:WAIT_PG
docker inspect --format "{{.State.Health.Status}}" etl_postgres 2>nul | findstr "healthy" >nul
if errorlevel 1 (
    timeout /t 3 /nobreak >nul
    echo   ... đang đợi
    goto WAIT_PG
)
echo [OK] PostgreSQL ready

echo.
echo Bước 2/3: Airflow init (migrate DB + tạo admin user)...
docker compose -f "%COMPOSE%" up airflow-init

echo.
echo Bước 3/3: Airflow webserver + scheduler...
docker compose -f "%COMPOSE%" up -d airflow-webserver airflow-scheduler

echo.
echo ╔══════════════════════════════════════════════════════╗
echo ║  Stack ready!                                        ║
echo ║                                                      ║
echo ║  Airflow UI : http://localhost:8080                  ║
echo ║  Login      : admin / admin (đọc trong .env)         ║
echo ║                                                      ║
echo ║  Postgres   : localhost:5432                         ║
echo ║  Database   : northwind_dw                           ║
echo ║  User       : etl_user / etl_password (đọc .env)     ║
echo ╚══════════════════════════════════════════════════════╝
pause & goto MENU

:STOP
docker compose -f "%COMPOSE%" stop
echo [OK] Stack đã dừng. Data giữ nguyên trong Docker volume.
pause & goto MENU

:STATUS
docker compose -f "%COMPOSE%" ps
pause & goto MENU

:LOGS
echo Services: postgres / airflow-webserver / airflow-scheduler
set /p "S=Service nào (Enter = all): "
if "%S%"=="" (
    docker compose -f "%COMPOSE%" logs -f --tail=50
) else (
    docker compose -f "%COMPOSE%" logs -f --tail=50 %S%
)
goto MENU

:RESET
echo.
echo [WARNING] Xóa toàn bộ database! Gõ YES để xác nhận:
set /p "OK="
if not "%OK%"=="YES" goto MENU
docker compose -f "%COMPOSE%" down -v
echo [OK] Đã reset. Chọn 1 để khởi động lại.
pause & goto MENU

:VENV
where python >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python không có trong PATH. Cài Python 3.11+ trước.
    pause & goto MENU
)
if exist "%VENV%" (
    echo .venv đã tồn tại. Tạo lại? (y/N)
    set /p "OK="
    if /i "!OK!"=="y" rmdir /s /q "%VENV%" else goto MENU
)
python -m venv "%VENV%"
"%VENV%\Scripts\pip" install --upgrade pip --quiet
"%VENV%\Scripts\pip" install -r "%ROOT%requirements.txt"
echo [OK] venv ready
pause & goto MENU

:SHELL
if not exist "%VENV%\Scripts\activate.bat" (
    echo [ERROR] Venv chưa setup. Chọn 6 trước.
    pause & goto MENU
)
cmd /k ""%VENV%\Scripts\activate.bat" && set PYTHONPATH=%ROOT% && echo. && echo [venv active] PYTHONPATH=%ROOT%"
goto MENU

:SEED
if not exist "%VENV%\Scripts\python.exe" (
    echo [ERROR] Venv chưa setup. Chọn 6 trước.
    pause & goto MENU
)
"%VENV%\Scripts\python" "%ROOT%scripts\download_seed.py"
pause & goto MENU

:VERIFY
if not exist "%VENV%\Scripts\python.exe" (
    echo [ERROR] Venv chưa setup. Chọn 6 trước.
    pause & goto MENU
)
"%VENV%\Scripts\python" "%ROOT%scripts\verify_sources.py"
pause & goto MENU

:RUN
if not exist "%VENV%\Scripts\python.exe" (
    echo [ERROR] Venv chưa setup. Chọn 6 trước.
    pause & goto MENU
)
echo Chạy pipeline...
set PYTHONPATH=%ROOT%
"%VENV%\Scripts\python" -m src.orchestration.pipeline run
pause & goto MENU

:AIRFLOW
start http://localhost:8080
goto MENU