package com.example.kafka;

public class KeyData {
    private String originalTopic;
    private String identifier;
    private String events;

    public String getOriginalTopic() { return originalTopic; }
    public void setOriginalTopic(String originalTopic) { this.originalTopic = originalTopic; }

    public String getIdentifier() { return identifier; }
    public void setIdentifier(String identifier) { this.identifier = identifier; }

    public String getEvents() { return events; }
    public void setEvents(String events) { this.events = events; }
}