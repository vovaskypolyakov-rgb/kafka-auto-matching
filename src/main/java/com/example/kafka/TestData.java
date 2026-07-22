package com.example.kafka;

public class TestData {
    private int rowIndex;          // номер строки в Excel (индекс, 0-based)
    private String keyCombination;
    private String expectedResult;   // ожидаемый matching_result
    private String expectedStatus;   // ожидаемый статус (из колонки E)
    private String valueJson;
    private String headersJson;

    public TestData() {}

    public TestData(int rowIndex, String keyCombination, String expectedResult,
                    String expectedStatus, String valueJson, String headersJson) {
        this.rowIndex = rowIndex;
        this.keyCombination = keyCombination;
        this.expectedResult = expectedResult;
        this.expectedStatus = expectedStatus;
        this.valueJson = valueJson;
        this.headersJson = headersJson;
    }

    public int getRowIndex() {
        return rowIndex;
    }

    public void setRowIndex(int rowIndex) {
        this.rowIndex = rowIndex;
    }

    public String getKeyCombination() {
        return keyCombination;
    }

    public void setKeyCombination(String keyCombination) {
        this.keyCombination = keyCombination;
    }

    public String getExpectedResult() {
        return expectedResult;
    }

    public void setExpectedResult(String expectedResult) {
        this.expectedResult = expectedResult;
    }

    public String getExpectedStatus() {
        return expectedStatus;
    }

    public void setExpectedStatus(String expectedStatus) {
        this.expectedStatus = expectedStatus;
    }

    public String getValueJson() {
        return valueJson;
    }

    public void setValueJson(String valueJson) {
        this.valueJson = valueJson;
    }

    public String getHeadersJson() {
        return headersJson;
    }

    public void setHeadersJson(String headersJson) {
        this.headersJson = headersJson;
    }

    @Override
    public String toString() {
        return keyCombination + " (ожидается " + expectedResult + "/" + expectedStatus + ")";
    }
}