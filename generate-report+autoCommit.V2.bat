@echo off
echo ========================================
echo 0. Проверка наличия history.json в корне...
if not exist "history" mkdir history
if not exist "history\history.json" (
    echo Создаём пустой history.json...
    echo [] > history\history.json
)

echo ========================================
echo 1. Запуск тестов...
call mvn clean test

echo ========================================
echo 2. Копирование истории из корня в результаты (перед генерацией)...
if exist "history\history.json" (
    copy history\history.json target\allure-results\
) else (
    echo history.json не найден в корне, пропускаем...
)

echo ========================================
echo 3. Генерация Allure-отчёта...
call mvn allure:report

echo ========================================
echo 4. Обновление истории после генерации (если Allure её обновил)...
if exist "target\allure-results\history.json" (
    copy target\allure-results\history.json history\
)

echo ========================================
echo Готово! Отчёт: https://vovaskypolyakov-rgb.github.io/kafka-auto-matching/allure-report/
echo Не забудьте запушить: git add docs/ history/ && git commit -m "Обновлён отчёт" && git push
pause