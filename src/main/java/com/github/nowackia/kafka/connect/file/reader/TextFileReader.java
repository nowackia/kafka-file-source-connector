package com.github.nowackia.kafka.connect.file.reader;

import com.github.nowackia.kafka.connect.config.FsSourceTaskConfig;
import com.github.nowackia.kafka.connect.file.FileMetadata;
import org.apache.hadoop.fs.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class TextFileReader extends AbstractFileReader {

    private static final Logger log = LoggerFactory.getLogger(TextFileReader.class);

    private LineNumberReader reader;
    private String currentLine;

    public TextFileReader(FileSystem fs, FileMetadata metadata, String cobDate, FsSourceTaskConfig config) throws IOException {
        super(fs, metadata, cobDate, config);
        InputStream inputStream = getInputStream(fs, metadata.getPath());

        this.reader = new LineNumberReader(new InputStreamReader(inputStream, Charset.defaultCharset()));
    }

    @Override
    protected Schema constructValueSchema(String path) throws IOException {
        return SchemaBuilder.struct()
                .field("line", Schema.STRING_SCHEMA)
                .build();
    }

    @Override
    public boolean isBeginning() {
        return (reader.getLineNumber() <= 1);
    }

    @Override
    public boolean hasNext() {
        if (finished) {
            return false;
        } else if (currentLine != null ) {
            return true;
        } else {
            try {
                while (true) {
                    String line = reader.readLine();
                    offset.setOffset(reader.getLineNumber());
                    if (line == null) {
                        finished = true;
                        return false;
                    }

                    currentLine = line;
                    return true;
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
            if (offset < reader.getLineNumber()) {
                InputStream inputStream = getInputStream(fs, getFilePath());
                this.reader = new LineNumberReader(new InputStreamReader(inputStream, Charset.defaultCharset()));

                currentLine = null;
            }
            while ((currentLine = reader.readLine()) != null) {
                if (reader.getLineNumber() - 1 == offset) {
                    this.offset.setOffset(reader.getLineNumber());
                    return;
                }
            }
            this.offset.setOffset(reader.getLineNumber());
        } catch (IOException ioe) {
            throw new ConnectException("Error seeking file " + getFilePath(), ioe);
        }
    }

    @Override
    protected Map.Entry<Struct, Struct> nextRecord() {
        if (!hasNext())
            throw new NoSuchElementException("There are no more records in file: " + getFilePath());

        Struct valueStruct = new Struct(valueSchema);
        valueStruct.put(valueSchema.fields().get(0), currentLine);
        currentLine = null;

        Struct keyStruct = constructKeySchema(source, name, cobDate, "text");
        if(isBeginning())
            keyStruct = makeFirst(keyStruct);
        if(!hasNext())
            keyStruct = makeLast(keyStruct);

        return new AbstractMap.SimpleEntry<>(keyStruct, valueStruct);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
