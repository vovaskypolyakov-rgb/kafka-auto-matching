@echo off
echo ========================================
echo Запуск тестов...
call mvn clean test

echo ========================================
echo Подготовка истории для отчёта...
if exist "history" (
    echo Копирование сохранённой истории в результаты...
    xcopy history target\allure-results\history /E /I /Y
) else (
    echo Папка history не найдена, создаём...
    mkdir history
)

echo ========================================
echo Генерация Allure-отчёта...
call mvn allure:report

echo ========================================
echo Сохранение обновлённой истории...
if exist "target\allure-results\history" (
    xcopy target\allure-results\history history /E /I /Y
) else (
    echo Папка истории не создалась (возможно, нет данных)
)

echo ========================================
echo Готово! Отчёт доступен по ссылке:
echo https://vovaskypolyakov-rgb.github.io/kafka-auto-matching/allure-report/
echo.
echo Не забудьте запушить изменения:
echo git add docs/allure-report/ history/
echo git commit -m "Обновлён Allure-отчёт"
echo git push
pause