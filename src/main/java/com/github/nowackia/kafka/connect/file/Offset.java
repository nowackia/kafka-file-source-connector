package com.github.nowackia.kafka.connect.file;

/**
 * Offset information on read files
 */
public interface Offset {

    long getRecordOffset();

}
