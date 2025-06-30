@echo off
chcp 65001 >nul
echo.
echo ========================================
echo   ЭКСПОРТ ТОВАРОВ ИЗ ЯНДЕКС.МАРКЕТА
echo ========================================
echo.

REM Проверяем наличие Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python не найден!
    echo 💡 Установите Python с официального сайта: https://python.org
    pause
    exit /b 1
)

REM Проверяем наличие конфигурации
if not exist config.env (
    echo ❌ Файл config.env не найден!
    echo 💡 Скопируйте config.env.example в config.env и заполните данные
    pause
    exit /b 1
)

echo 🚀 Запуск экспорта товаров...
echo.

REM Запускаем упрощенный скрипт
python simple_export_products.py

echo.
echo ✅ Готово! Проверьте созданный CSV файл.
echo.
pause 