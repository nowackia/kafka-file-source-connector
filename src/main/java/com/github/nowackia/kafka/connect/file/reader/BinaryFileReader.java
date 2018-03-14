package com.github.nowackia.kafka.connect.file.reader;

import com.github.nowackia.kafka.connect.config.FsSourceTaskConfig;
import com.github.nowackia.kafka.connect.file.FileMetadata;
import com.github.nowackia.kafka.connect.file.stream.PositionInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;

public class BinaryFileReader extends AbstractFileReader {

    private static final Logger log = LoggerFactory.getLogger(TextFileReader.class);

    private static final int BUFFER_SIZE = 4096; //4KB

    private PositionInputStream stream;
    private byte[] buffer = new byte[BUFFER_SIZE];
    private int read = 0;

    public BinaryFileReader(FileSystem fs, FileMetadata metadata, String cobDate, FsSourceTaskConfig config) throws IOException {
        super(fs, metadata, cobDate, config);
        InputStream inputStream = getInputStream(fs, metadata.getPath());
        this.stream = new PositionInputStream(inputStream);

    }

    @Override
    protected Schema constructValueSchema(String path) throws IOException {
        return SchemaBuilder.struct()
                .field("bytes", Schema.BYTES_SCHEMA)
                .field("len", Schema.INT32_SCHEMA)
                .build();
    }

    @Override
    public boolean isBeginning() {
        return stream.getPosition() <= 0;
    }

    @Override
    public boolean hasNext() {
        if (finished) {
            return false;
        } else if (read > 0) {
            return true;
        } else {
            try {
                read = stream.read(buffer, 0, buffer.length);
                offset.setOffset(stream.getPosition());
                if (read > 0) {
                    return true;
                } else {
                    finished = true;
                    return false;
                }
            } catch (IOException ioe) {
                throw new IllegalStateException(ioe);
            }
        }
    }

    @Override
    public void seek(Long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("Record offset must be greater than 0");
        }
        try {
            InputStream inputStream = getInputStream(fs, getFilePath());
            this.stream = new PositionInputStream(inputStream);
            clearBuffer();

            this.stream.skip(offset);
            this.offset.setOffset(stream.getPosition());
        } catch (IOException ioe) {
            throw new ConnectException("Error seeking file " + getFilePath(), ioe);
        }
    }

    @Override
    protected Map.Entry<Struct, Struct> nextRecord() {
        if (!hasNext())
            throw new NoSuchElementException("There are no more records in file: " + getFilePath());

        byte[] values = new byte[buffer.length];
        System.arraycopy( buffer, 0, values, 0, read );

        Struct valueStruct = new Struct(valueSchema);
        valueStruct.put(valueSchema.fields().get(0), values);
        valueStruct.put(valueSchema.fields().get(1), read);
        clearBuffer();

        Struct keyStruct = constructKeySchema(source, name, cobDate, "binary");
        if(isBeginning())
            keyStruct = makeFirst(keyStruct);
        if(!hasNext())
            keyStruct = makeLast(keyStruct);

        return new AbstractMap.SimpleEntry<>(keyStruct, valueStruct);
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }

    protected long countSize(FileSystem fs, FileMetadata metadata) throws IOException {
        return metadata.getLen();
    }

    private void clearBuffer() {
        Arrays.fill(buffer, (byte)0);
        read = 0;
    }
}
