@echo off
echo ========================================
echo 1. Запуск тестов...
call mvn clean test

echo ========================================
echo 2. Подготовка истории...
if exist "target\allure-results\history" (
    echo Копирование истории из результатов в корень...
    xcopy target\allure-results\history history /E /I /Y
) else (
    echo Папка истории в результатах не найдена.
    echo Если это первый запуск, создаём историю из результатов...
    if not exist "history" mkdir history
    copy target\allure-results\*.json history\ 2>nul
)

echo ========================================
echo 3. Копирование сохранённой истории в результаты (перед генерацией)...
if exist "history" (
    xcopy history target\allure-results\history /E /I /Y
)

echo ========================================
echo 4. Генерация Allure-отчёта...
call mvn allure:report

echo ========================================
echo 5. Сохранение обновлённой истории...
if exist "target\allure-results\history" (
    xcopy target\allure-results\history history /E /I /Y
)

echo ========================================
echo Готово! Отчёт: https://vovaskypolyakov-rgb.github.io/kafka-auto-matching/allure-report/
echo Не забудьте запушить: git add docs/ history/ && git commit -m "Обновлён отчёт" && git push
pause