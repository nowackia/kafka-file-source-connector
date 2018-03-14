package com.github.nowackia.kafka.connect.file.reader;

import com.github.nowackia.kafka.connect.config.FsSourceTaskConfig;
import com.github.nowackia.kafka.connect.file.FileMetadata;
import com.github.nowackia.kafka.connect.file.Offset;
import org.apache.hadoop.fs.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public abstract class AbstractFileReader implements FileReader {

    protected FileSystem fs;

    protected String name;
    protected String cobDate;

    protected Schema keySchema;
    protected Schema valueSchema;

    protected final TextOffset offset;
    protected final FsSourceTaskConfig config;

    protected boolean finished;
    protected String source;

    private short percentageRead;
    private final String filePath;
    private final long size;


    public AbstractFileReader(FileSystem fs, FileMetadata metadata, String cobDate, FsSourceTaskConfig config) throws IOException {
        if (metadata.getPath() == null) {
            throw new IllegalArgumentException("filePath is required");
        }
        this.fs = fs;
        this.filePath = metadata.getPath();
        this.size = this.countSize(fs, metadata);
        this.config = config;

        this.offset = new TextOffset(0);
        this.name = Paths.get(filePath).getFileName().toString();
        this.cobDate = cobDate;
        this.source = config.getSource();

        this.finished = false;
        this.percentageRead = 0;

        this.keySchema = constructKeySchema();
        this.valueSchema = constructValueSchema(filePath);
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public String getFilePath() {
        return filePath;
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public Offset currentOffset() {
        return offset;
    }

    @Override
    public String getCobDate() {
        return cobDate;
    }

    protected InputStream getInputStream(FileSystem fs, String path) throws IOException {
        InputStream inputStream = null;

        if (fs == null)
            inputStream = new FileInputStream(path);
        else
            inputStream = fs.open(new org.apache.hadoop.fs.Path(path));

        return inputStream;
    }

    protected long countSize(FileSystem fs, FileMetadata metadata) throws IOException {
        /* Get the number of lines in the file */
        InputStream is = new BufferedInputStream(getInputStream(fs, metadata.getPath()));
        try {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
            is.close();
        }
    }

    protected Struct constructKeySchema(String source, String name, String cobDate, String fileType) {
        Struct keyStruct = new Struct(keySchema)
                .put(keySchema.fields().get(0), source)
                .put(keySchema.fields().get(1), name)
                .put(keySchema.fields().get(2), this.getSize())
                .put(keySchema.fields().get(3), cobDate)
                .put(keySchema.fields().get(4), fileType)
                .put(keySchema.fields().get(5), false) /* first */
                .put(keySchema.fields().get(6), false); /* last */

        return keyStruct;
    }

    protected Struct makeFirst(Struct keyStruct) {
        keyStruct.put(keySchema.fields().get(5), true); /* first */
        return keyStruct;
    }

    protected Struct makeLast(Struct keyStruct) {
        keyStruct.put(keySchema.fields().get(6), true); /* last */
        return keyStruct;
    }

    protected Schema constructKeySchema() {
        return SchemaBuilder.struct()
                .field("source", Schema.STRING_SCHEMA)
                .field("file-name", Schema.STRING_SCHEMA)
                .field("file-total-count", Schema.INT64_SCHEMA)
                .field("cob-date", Schema.STRING_SCHEMA)
                .field("file-type", Schema.STRING_SCHEMA)
                .field("first-field", Schema.BOOLEAN_SCHEMA)
                .field("last-field", Schema.BOOLEAN_SCHEMA)
                .build();
    }

    protected abstract Schema constructValueSchema(String filePath) throws IOException;

    public final Map.Entry<Struct, Struct> next() {
        return nextRecord();
    }

    protected abstract Map.Entry<Struct, Struct> nextRecord();

    private short percentage(long value, long max, int interval) {
        int percentage = (int)((((double)value / (double)max) * 100.0) / interval) * interval;
        return (short)Math.min(percentage, 100);
    };

    public void logProgress(Logger log, int interval) {
        long read = Math.max(this.offset.getRecordOffset() - 1, 0);
        long count = this.getSize();

        short value = percentage(read, count, interval);
        if (value != percentageRead) {
            percentageRead = value;
            log.info("File " + this.getFilePath() + " processed in over " + percentageRead + "% [" + read + " from " + count + "]");
        }
    }
}
