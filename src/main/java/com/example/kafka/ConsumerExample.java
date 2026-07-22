package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerExample {

    private static final String BOOTSTRAP_SERVERS = "10.65.242.182:9092,10.65.242.145:9092,10.65.242.181:9092,10.65.242.183:9092,10.65.242.180:9092";
    private static final String TOPIC_EVENT = "zdrav.document-remd-proc.event.priv";
    private static final String EXPECTED_MATCHING = "11111111";
    private static final String EXPECTED_STATUS = "SUCCESS";
    private static final int TIMEOUT_SECONDS = 15;

    /**
     * Ожидает сообщение в указанном топике с заданным identifier, matching_result и status.
     *
     * @param topic             имя топика для чтения
     * @param bootstrapServers  адреса брокеров
     * @param expectedIdentifier ожидаемое значение заголовка identifier
     * @param expectedMatching  ожидаемое значение matching_result
     * @param expectedStatus    ожидаемое значение status
     * @param timeoutSeconds    таймаут в секундах
     * @return true, если сообщение найдено, иначе false
     */
    public static boolean waitForResponse(String topic, String bootstrapServers,
                                          String expectedIdentifier,
                                          String expectedMatching,
                                          String expectedStatus,
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

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        long startTime = System.currentTimeMillis();
        boolean found = false;

        while (System.currentTimeMillis() - startTime < timeoutSeconds * 1000L) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                String identifier = null;
                String matchingResult = null;
                String status = null;

                if (record.headers() != null) {
                    for (Header header : record.headers()) {
                        String key = header.key();
                        String value = new String(header.value());
                        switch (key) {
                            case "identifier": identifier = value; break;
                            case "matching_result": matchingResult = value; break;
                            case "status": status = value; break;
                        }
                    }
                }

                if (expectedIdentifier.equals(identifier) &&
                        expectedMatching.equals(matchingResult) &&
                        expectedStatus.equals(status)) {
                    found = true;
                    System.out.println("✅ Найден подходящий ответ!");
                    System.out.println("  Key: " + record.key());
                    System.out.println("  Value: " + record.value());
                    System.out.println("  Headers: identifier=" + identifier +
                            ", matching_result=" + matchingResult +
                            ", status=" + status);
                    break;
                }
            }
            if (found) break;
        }

        consumer.close();
        return found;
    }

    /**
     * Основной метод для ручного запуска (использует correlationId из системного свойства).
     */
    public static void main(String[] args) {
        String expectedIdentifier = System.getProperty("correlationId");
        if (expectedIdentifier == null || expectedIdentifier.isEmpty()) {
            System.err.println("Correlation ID не найден. Запустите сначала ProducerExample.");
            return;
        }

        boolean found = waitForResponse(
                TOPIC_EVENT,
                BOOTSTRAP_SERVERS,
                expectedIdentifier,
                EXPECTED_MATCHING,
                EXPECTED_STATUS,
                TIMEOUT_SECONDS
        );

        if (found) {
            System.out.println("✅ Ответ получен.");
        } else {
            System.err.println("❌ Ответ не найден за " + TIMEOUT_SECONDS + " секунд.");
        }
    }
}