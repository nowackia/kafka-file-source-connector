package com.github.nowackia.kafka.connect.policy;

import com.github.nowackia.kafka.connect.file.FileMetadata;
import com.github.nowackia.kafka.connect.file.reader.FileReader;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Policy specifying on how to read the files
 */
public interface Policy extends Closeable {

    Set<FileMetadata> execute(OffsetStorageReader offsetStorageReader) throws IOException;

    void incrementExecutions();

    FileReader offer(FileMetadata metadata, OffsetStorageReader offsetStorageReader) throws IOException;

    boolean hasEnded();

    String getURI();

    List<String> getFilters();

    void interrupt();
}