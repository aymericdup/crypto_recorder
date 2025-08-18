@echo off
echo Installing quantpylib...


python -m venv crypto_recorder_venv

crypto_recorder_venv\Scripts\activate
pip install Cython
pip install -e D:\IT\Github\quantpylib\quantpylib

crypto_recorder_venv deactivate

if %errorlevel% equ 0 (
    echo.
    echo ✓ quantpylib installed successfully!
) else (
    echo.
    echo ✗ Installation failed!
)

pause