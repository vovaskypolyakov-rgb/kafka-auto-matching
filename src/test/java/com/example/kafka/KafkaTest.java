package com.example.kafka;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.qameta.allure.Allure;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import io.qameta.allure.Story;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;

public class KafkaTest {

    private static final Logger log = LoggerFactory.getLogger(KafkaTest.class);
    private static final String BOOTSTRAP_SERVERS = "10.65.242.182:9092,10.65.242.145:9092,10.65.242.181:9092,10.65.242.183:9092,10.65.242.180:9092";
    private static final String TOPIC_PIPE = "zdrav.document-remd-proc.pipe.priv";
    private static final String TOPIC_EVENT = "zdrav.document-remd-proc.event.priv";
    private static final int TIMEOUT_SECONDS = 15;

    static Stream<TestData> testDataProvider() throws Exception {
        return ExcelDataReader.readTestData("kafka-auto-matching.xlsx").stream();
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("testDataProvider")
    @Feature("Kafka интеграция")
    @Story("Автоматическое сопоставление по данным из Excel")
    @Severity(SeverityLevel.CRITICAL)
    public void testSendAndReceive(TestData testData) throws Exception {
        String correlationId = UUID.randomUUID().toString();
        log.info("Запуск теста для комбинации: {}, correlationId: {}", testData.getKeyCombination(), correlationId);

        Gson gson = new Gson();
        JsonObject headersJson = gson.fromJson(testData.getHeadersJson(), JsonObject.class);
        headersJson.addProperty("identifier", correlationId);
        String events = headersJson.has("events") ? headersJson.get("events").getAsString() : "newEmrd";

        Map<String, String> keyMap = new HashMap<>();
        keyMap.put("originalTopic", TOPIC_PIPE);
        keyMap.put("identifier", correlationId);
        keyMap.put("events", events);
        String keyJson = gson.toJson(keyMap);

        List<Header> headers = new ArrayList<>();
        for (Map.Entry<String, JsonElement> entry : headersJson.entrySet()) {
            headers.add(new RecordHeader(entry.getKey(), entry.getValue().getAsString().getBytes()));
        }

        Allure.step("Отправка сообщения для комбинации: " + testData.getKeyCombination(), () -> {
            Allure.addAttachment("Отправленный Value (JSON)", "application/json", testData.getValueJson());
            Allure.addAttachment("Отправленный Key (JSON)", "application/json", keyJson);
            Allure.addAttachment("Отправленные заголовки", "text/plain", headersJson.toString());

            ProducerExample.sendRawMessage(TOPIC_PIPE, BOOTSTRAP_SERVERS, keyJson,
                    testData.getValueJson(), headers);
        });

        Allure.step("Ожидание ответа с matching_result=" +
                (testData.getExpectedResult().isEmpty() ? "<любой>" : testData.getExpectedResult()) +
                " и статусом " + testData.getExpectedStatus(), () -> {

            String expectedMatching = testData.getExpectedResult().isEmpty() ? null : testData.getExpectedResult();

            ResponseData response = waitForResponse(
                    TOPIC_EVENT,
                    BOOTSTRAP_SERVERS,
                    correlationId,
                    expectedMatching,
                    TIMEOUT_SECONDS
            );

            Allure.addAttachment("Ответ (Value)", "application/json", response.value);
            Allure.addAttachment("Ответ (Headers)", "text/plain", response.headers);

            // Выводим в лог фактические значения (без записи в Excel)
            log.info("Фактический matching_result: {}, фактический статус: {}", response.actualResult, response.actualStatus);

            Assertions.assertTrue(response.found,
                    "Не найден ответ с identifier=" + correlationId +
                            (expectedMatching == null ? "" : " и matching_result=" + expectedMatching));

            // Проверяем matching_result, если он ожидается
            if (expectedMatching != null) {
                Assertions.assertEquals(expectedMatching, response.actualResult,
                        "matching_result не совпадает");
            }

            // Проверяем статус, если он указан
            if (testData.getExpectedStatus() != null && !testData.getExpectedStatus().isEmpty()) {
                Assertions.assertEquals(testData.getExpectedStatus(), response.actualStatus,
                        "статус не совпадает с ожидаемым");
            } else {
                log.warn("Ожидаемый статус в Excel не задан, проверка статуса пропущена.");
            }
        });
    }

    private ResponseData waitForResponse(String topic, String bootstrapServers,
                                         String expectedIdentifier,
                                         String expectedMatching,
                                         int timeoutSeconds) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "event-checker-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < timeoutSeconds * 1000L) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    String identifier = null;
                    String matchingResult = null;
                    String status = null;
                    StringBuilder headersBuilder = new StringBuilder();

                    if (record.headers() != null) {
                        for (Header header : record.headers()) {
                            String key = header.key();
                            String value = new String(header.value());
                            headersBuilder.append(key).append(": ").append(value).append("\n");
                            switch (key) {
                                case "identifier": identifier = value; break;
                                case "matching_result": matchingResult = value; break;
                                case "status": status = value; break;
                            }
                        }
                    }

                    boolean match = expectedIdentifier.equals(identifier);
                    if (expectedMatching != null) {
                        match = match && expectedMatching.equals(matchingResult);
                    }
                    if (match) {
                        return new ResponseData(true, record.value(), headersBuilder.toString(),
                                matchingResult, status);
                    }
                }
            }
            return new ResponseData(false, "", "", null, null);
        }
    }

    private static class ResponseData {
        boolean found;
        String value;
        String headers;
        String actualResult;
        String actualStatus;

        ResponseData(boolean found, String value, String headers, String actualResult, String actualStatus) {
            this.found = found;
            this.value = value;
            this.headers = headers;
            this.actualResult = actualResult;
            this.actualStatus = actualStatus;
        }
    }
}