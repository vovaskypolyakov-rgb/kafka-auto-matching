package com.example.kafka;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class ProducerExample {

    private static final String BOOTSTRAP_SERVERS = "10.65.242.182:9092,10.65.242.145:9092,10.65.242.181:9092,10.65.242.183:9092,10.65.242.180:9092";
    private static final String TOPIC_PIPE = "zdrav.document-remd-proc.pipe.priv";

    /**
     * Отправляет сообщение с указанным correlationId.
     * Для отправки используется фиксированный набор данных из buildPatientData().
     */
    public static void sendMessage(String topic, String bootstrapServers, String correlationId) {
        PatientData valueData = buildPatientData();
        sendMessageWithData(topic, bootstrapServers, correlationId, valueData);
    }

    /**
     * Отправляет сообщение с переданным объектом PatientData.
     */
    public static void sendMessageWithData(String topic, String bootstrapServers, String correlationId, PatientData patientData) {
        Gson gson = new Gson();

        KeyData keyData = new KeyData();
        keyData.setOriginalTopic(topic);
        keyData.setIdentifier(correlationId);
        keyData.setEvents("newEmrd");
        String keyJson = gson.toJson(keyData);

        String valueJson = gson.toJson(patientData);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader("identifier", correlationId.getBytes()));
        headers.add(new RecordHeader("events", "newEmrd".getBytes()));

        ProducerRecord<String, String> record = new ProducerRecord<>(
                topic,
                null,
                keyJson,
                valueJson,
                headers
        );

        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.println("Сообщение отправлено в " + topic +
                        ", offset = " + metadata.offset() +
                        ", identifier = " + correlationId);
            } else {
                System.err.println("Ошибка отправки: " + exception.getMessage());
            }
        });

        producer.flush();
        producer.close();
    }

    /**
     * Возвращает фиксированный объект PatientData (без генерации).
     */
    public static PatientData buildPatientData() {
        PatientData data = new PatientData();
        data.setEmdrId("224.16.25.09.000241002");
        data.setKind("224");
        data.setOrganization("1.2.643.5.1.13.13.12.2.16.1080");
        data.setCreationDateTime("2025-10-07T08:25:38.000+00:00");
        data.setPatientSnils("33276225363");
        data.setRegOkato("92000000000");
        data.setRegionCode("16");
        data.setSigner(new String[0]);
        data.setProcessingStatus("NEW");
        data.setSupplierId("emdr-rmis-1");
        data.setSupplierDocId("77700268-6023-4779-9902-000840bf1516");
        data.setDeltaMessageTime("2025-11-24T08:48:14.000+00:00");

        Patient patient = new Patient();
        patient.setSurname("Красильникова");
        patient.setName("Ольга");
        patient.setPatrName("Александровна");
        patient.setBirthDate("1980-09-05");
        patient.setSnils("33276225363");
        patient.setEnp("1851982082633279");

        OtherId otherId = new OtherId();
        otherId.setNumber("8081342910");
        otherId.setType("1");
        patient.setOtherId(otherId);
        data.setPatient(patient);

        Representative rep = new Representative();
        rep.setSurname("");
        rep.setName("");
        rep.setPatrName("");
        rep.setSnils("");
        data.setRepresentative(rep);

        data.setDocNumber("0303202301");
        data.setDocSeries("45");
        return data;
    }

    public static void main(String[] args) {
        String correlationId = UUID.randomUUID().toString();
        sendMessage(TOPIC_PIPE, BOOTSTRAP_SERVERS, correlationId);
        System.setProperty("correlationId", correlationId);
        System.out.println("Correlation ID сохранён: " + correlationId);
    }
    /**
     * Отправляет сообщение с готовыми JSON-строками для ключа и значения.
     *
     * @param topic             топик
     * @param bootstrapServers  адреса брокеров
     * @param keyJson           ключ в формате JSON
     * @param valueJson         значение в формате JSON
     * @param headers           список заголовков (может быть null)
     */
    public static void sendRawMessage(String topic, String bootstrapServers,
                                      String keyJson, String valueJson,
                                      List<Header> headers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        ProducerRecord<String, String> record = new ProducerRecord<>(
                topic,
                null,
                keyJson,
                valueJson,
                headers
        );

        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.println("Сообщение отправлено в " + topic +
                        ", offset = " + metadata.offset());
            } else {
                System.err.println("Ошибка отправки: " + exception.getMessage());
            }
        });

        producer.flush();
        producer.close();
    }
}