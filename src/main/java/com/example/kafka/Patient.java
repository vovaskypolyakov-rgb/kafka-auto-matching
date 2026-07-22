package com.example.kafka;

public class Patient {
    private String surname;
    private String name;
    private String patrName;
    private String birthDate;
    private String snils;
    private String enp;
    private OtherId otherId;

    public String getSurname() { return surname; }
    public void setSurname(String surname) { this.surname = surname; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getPatrName() { return patrName; }
    public void setPatrName(String patrName) { this.patrName = patrName; }

    public String getBirthDate() { return birthDate; }
    public void setBirthDate(String birthDate) { this.birthDate = birthDate; }

    public String getSnils() { return snils; }
    public void setSnils(String snils) { this.snils = snils; }

    public String getEnp() { return enp; }
    public void setEnp(String enp) { this.enp = enp; }

    public OtherId getOtherId() { return otherId; }
    public void setOtherId(OtherId otherId) { this.otherId = otherId; }
}