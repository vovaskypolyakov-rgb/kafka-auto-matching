package com.example.kafka;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class ExcelDataReader {
    private static final Logger log = LoggerFactory.getLogger(ExcelDataReader.class);

    public static List<TestData> readTestData(String fileName) throws Exception {
        List<TestData> testDataList = new ArrayList<>();
        String filePath = System.getProperty("test.data.file", "./" + fileName);

        try (InputStream is = new FileInputStream(filePath)) {
            Workbook workbook = new XSSFWorkbook(is);
            Sheet sheet = workbook.getSheetAt(0); // первый лист

            int startRow = 2; // строки с данными начинаются с индекса 2
            for (int i = startRow; i <= sheet.getLastRowNum(); i++) {
                Row row = sheet.getRow(i);
                if (row == null) continue;

                Cell keyCell = row.getCell(1);
                if (keyCell == null || keyCell.getStringCellValue().trim().isEmpty()) continue;

                String keyCombination = getCellValue(row, 1);
                String expectedResult = getCellValue(row, 2);
                String expectedStatus = getCellValue(row, 4);
                String valueJson = getCellValue(row, 6);
                String headersJson = getCellValue(row, 7);

                // Пропускаем только если ключ пуст.
                // expectedResult может быть пустым (для кейсов без matching_result)
                if (keyCombination.isEmpty()) {
                    log.info("Пропущена строка {}: пустой ключ", i);
                    continue;
                }

                TestData data = new TestData(i, keyCombination, expectedResult,
                        expectedStatus, valueJson, headersJson);
                testDataList.add(data);
                log.info("Загружена строка {}: {}", i, keyCombination);
            }
            workbook.close();
        }
        return testDataList;
    }

    private static String getCellValue(Row row, int index) {
        Cell cell = row.getCell(index);
        if (cell == null) return "";
        switch (cell.getCellType()) {
            case STRING: return cell.getStringCellValue().trim();
            case NUMERIC: return String.valueOf((long) cell.getNumericCellValue());
            default: return "";
        }
    }
}