package com.github.nowackia.kafka.connect.file.reader;

import com.github.nowackia.kafka.connect.file.Offset;

public class TextOffset implements Offset {
    private long offset;

    public TextOffset(long offset) {
        this.offset = offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public long getRecordOffset() {
        return offset;
    }
}
