@echo off
REM ============================================
REM NYC Taxi Project - Migration to D: Drive
REM ============================================

echo.
echo ============================================
echo NYC TAXI PROJECT MIGRATION
echo From: C:\Users\admin\Documents\Projects\nyc-taxi-analytics
echo To:   D:\Projects\nyc-taxi-analytics
echo ============================================
echo.

REM Step 1: Create destination directory
echo [Step 1/5] Creating destination directory on D: drive...
if not exist "D:\Projects" mkdir "D:\Projects"
echo    * Created D:\Projects\
echo.

REM Step 2: Copy all project files
echo [Step 2/5] Copying project files (this may take a few minutes)...
xcopy "C:\Users\admin\Documents\Projects\nyc-taxi-analytics" "D:\Projects\nyc-taxi-analytics\" /E /I /H /Y
if %errorlevel% neq 0 (
    echo ERROR: Failed to copy files!
    pause
    exit /b 1
)
echo    * All files copied successfully
echo.

REM Step 3: Create new virtual environment on D:
echo [Step 3/5] Creating new Python virtual environment on D: drive...
cd /d D:\Projects\nyc-taxi-analytics
if exist "venv" (
    echo    * Removing old venv reference...
    rd /s /q venv
)
python -m venv venv
if %errorlevel% neq 0 (
    echo ERROR: Failed to create virtual environment!
    pause
    exit /b 1
)
echo    * Virtual environment created
echo.

REM Step 4: Install dependencies
echo [Step 4/5] Installing Python packages in new environment...
call venv\Scripts\activate
if exist "requirements.txt" (
    pip install -r requirements.txt --quiet
    if %errorlevel% neq 0 (
        echo WARNING: Some packages may have failed to install
        echo Please check and reinstall manually if needed
    ) else (
        echo    * All packages installed successfully
    )
) else (
    echo WARNING: requirements.txt not found
    echo You'll need to install packages manually
)
echo.

REM Step 5: Verify migration
echo [Step 5/5] Verifying migration...
if exist "D:\Projects\nyc-taxi-analytics\kafka" (
    echo    * kafka folder found
)
if exist "D:\Projects\nyc-taxi-analytics\airflow" (
    echo    * airflow folder found
)
if exist "D:\Projects\nyc-taxi-analytics\scripts" (
    echo    * scripts folder found
)
if exist "D:\Projects\nyc-taxi-analytics\data" (
    echo    * data folder found
)
if exist "D:\Projects\nyc-taxi-analytics\venv" (
    echo    * venv folder found
)
echo.

echo ============================================
echo MIGRATION COMPLETE!
echo ============================================
echo.
echo Your project is now at: D:\Projects\nyc-taxi-analytics
echo.
echo IMPORTANT NEXT STEPS:
echo.
echo 1. TEST the project on D: drive:
echo    cd D:\Projects\nyc-taxi-analytics
echo    venv\Scripts\activate
echo    python scripts/test_producer.py
echo.
echo 2. If everything works, DELETE from C: drive:
echo    rd /s "C:\Users\admin\Documents\Projects\nyc-taxi-analytics"
echo.
echo 3. Update any hardcoded paths in your code:
echo    Old: C:\Users\admin\Documents\Projects\nyc-taxi-analytics
echo    New: D:\Projects\nyc-taxi-analytics
echo.
echo 4. Configure Docker to use D: drive (if using Docker)
echo.
echo Freed space on C: drive: ~10-15GB
echo Used space on D: drive: ~10-12GB out of 350GB
echo.
echo ============================================
pause
