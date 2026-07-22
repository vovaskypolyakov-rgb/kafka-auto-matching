@echo off
echo Запуск тестов...
call mvn clean test

echo Генерация Allure-отчёта...
call mvn allure:report

echo Отчёт сгенерирован в docs/allure-report/
echo Ссылка: https://vovaskypolyakov-rgb.github.io/kafka-auto-matching/allure-report/
pause