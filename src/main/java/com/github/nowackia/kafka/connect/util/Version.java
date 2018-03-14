package com.github.nowackia.kafka.connect.util;

public enum Version {
    CURRENT ("1.0.0");

    private final String number;

    Version(String number) {
        this.number = number;
    }

    public String getNumber() {
        return number;
    }

    @Override
    public String toString() {
        return this.getNumber();
    }
}
