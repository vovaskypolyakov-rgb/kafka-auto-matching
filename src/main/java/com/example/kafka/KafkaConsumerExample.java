package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerExample {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_EVENT = "zdrav.document-remd-proc.event.priv";
    private static final String EXPECTED_IDENTIFIER = System.getProperty("correlationId", "abc"); // берём из системного свойства
    private static final String EXPECTED_MATCHING = "11111111";
    private static final String EXPECTED_STATUS = "SUCCESS";
    private static final int TIMEOUT_SECONDS = 15;

    public static void main(String[] args) {
        // Настройка консюмера
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "event-checker-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_EVENT));

        System.out.println("Ожидание ответа с identifier = " + EXPECTED_IDENTIFIER +
                ", matching_result = " + EXPECTED_MATCHING +
                ", status = " + EXPECTED_STATUS);

        long startTime = System.currentTimeMillis();
        boolean found = false;

        while (System.currentTimeMillis() - startTime < TIMEOUT_SECONDS * 1000) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                // Проверяем заголовки
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

                // Фильтр: совпадает identifier и нужные значения
                if (EXPECTED_IDENTIFIER.equals(identifier) &&
                        EXPECTED_MATCHING.equals(matchingResult) &&
                        EXPECTED_STATUS.equals(status)) {

                    System.out.println("Найден подходящий ответ:");
                    System.out.println("  Key: " + record.key());
                    System.out.println("  Value: " + record.value());
                    System.out.println("  Headers: identifier=" + identifier +
                            ", matching_result=" + matchingResult +
                            ", status=" + status);
                    found = true;
                    break; // выходим из цикла записей
                }
            }
            if (found) break;
        }

        if (!found) {
            System.err.println("Ответ не найден за " + TIMEOUT_SECONDS + " секунд.");
        }

        consumer.close();
    }
}