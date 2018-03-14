package com.github.nowackia.kafka.connect.file.reader;

import com.github.nowackia.kafka.connect.file.Offset;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

public interface FileReader extends Iterator<Map.Entry<Struct, Struct>>, Closeable {

    String getFilePath();

    boolean isBeginning();

    boolean isFinished();

    boolean hasNext();

    long getSize();

    Map.Entry<Struct, Struct> next();

    void seek(Long offset);

    Offset currentOffset();

    String getCobDate();

    void logProgress(Logger log, int interval);
}
