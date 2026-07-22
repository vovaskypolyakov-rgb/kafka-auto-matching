package com.example.kafka;

public class PatientData {
    private String emdrId;
    private String kind;
    private String organization;
    private String creationDateTime;
    private String patientSnils;
    private String regOkato;
    private String regionCode;
    private String[] signer;
    private String processingStatus;
    private String supplierId;
    private String supplierDocId;
    private String deltaMessageTime;
    private Patient patient;
    private Representative representative;
    private String docNumber;
    private String docSeries;

    // Геттеры и сеттеры
    public String getEmdrId() { return emdrId; }
    public void setEmdrId(String emdrId) { this.emdrId = emdrId; }

    public String getKind() { return kind; }
    public void setKind(String kind) { this.kind = kind; }

    public String getOrganization() { return organization; }
    public void setOrganization(String organization) { this.organization = organization; }

    public String getCreationDateTime() { return creationDateTime; }
    public void setCreationDateTime(String creationDateTime) { this.creationDateTime = creationDateTime; }

    public String getPatientSnils() { return patientSnils; }
    public void setPatientSnils(String patientSnils) { this.patientSnils = patientSnils; }

    public String getRegOkato() { return regOkato; }
    public void setRegOkato(String regOkato) { this.regOkato = regOkato; }

    public String getRegionCode() { return regionCode; }
    public void setRegionCode(String regionCode) { this.regionCode = regionCode; }

    public String[] getSigner() { return signer; }
    public void setSigner(String[] signer) { this.signer = signer; }

    public String getProcessingStatus() { return processingStatus; }
    public void setProcessingStatus(String processingStatus) { this.processingStatus = processingStatus; }

    public String getSupplierId() { return supplierId; }
    public void setSupplierId(String supplierId) { this.supplierId = supplierId; }

    public String getSupplierDocId() { return supplierDocId; }
    public void setSupplierDocId(String supplierDocId) { this.supplierDocId = supplierDocId; }

    public String getDeltaMessageTime() { return deltaMessageTime; }
    public void setDeltaMessageTime(String deltaMessageTime) { this.deltaMessageTime = deltaMessageTime; }

    public Patient getPatient() { return patient; }
    public void setPatient(Patient patient) { this.patient = patient; }

    public Representative getRepresentative() { return representative; }
    public void setRepresentative(Representative representative) { this.representative = representative; }

    public String getDocNumber() { return docNumber; }
    public void setDocNumber(String docNumber) { this.docNumber = docNumber; }

    public String getDocSeries() { return docSeries; }
    public void setDocSeries(String docSeries) { this.docSeries = docSeries; }
}