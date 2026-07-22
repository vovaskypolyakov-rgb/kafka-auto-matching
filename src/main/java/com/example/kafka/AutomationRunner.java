package com.example.kafka;

public class AutomationRunner {
    public static void main(String[] args) {
        // 1. Запускаем продюсера
        ProducerExample.main(args);

        // 2. Запускаем консюмера (он использует тот же correlationId из системного свойства)
        ConsumerExample.main(args);
    }
}