package com.github.nowackia.kafka.connect.file.reader;

import com.github.nowackia.kafka.connect.config.FsSourceTaskConfig;
import com.github.nowackia.kafka.connect.file.FileMetadata;
import com.github.nowackia.kafka.connect.schema.CsvSchemaGenerator;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
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
import org.apache.hadoop.fs.*;

public class CsvFileReader extends AbstractFileReader {

    private static final Logger log = LoggerFactory.getLogger(CsvFileReader.class);

    private CSVReader reader;
    private String[] currentLine;

    public CsvFileReader(FileSystem fs, FileMetadata metadata, String cobDate, FsSourceTaskConfig config) throws IOException {
        super(fs, metadata, cobDate, config);
        InputStream inputStream = getInputStream(fs, metadata.getPath());

        CSVParser csvParser = CsvSchemaGenerator.createCSVParserBuilder().build();
        this.reader = CsvSchemaGenerator.createCSVReaderBuilder(new InputStreamReader(inputStream, Charset.defaultCharset()), csvParser).build();
    }

    @Override
    protected Schema constructValueSchema(String filePath) throws IOException {
        InputStream inputStream = getInputStream(fs, filePath);

        CsvSchemaGenerator schemaGenerator = new CsvSchemaGenerator(filePath, inputStream);
        return schemaGenerator.generate();
    }

    @Override
    public boolean isBeginning() {
        return (reader.getLinesRead() <= 2);
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
                    String[] line = process(reader.readNext());
                    offset.setOffset(reader.getLinesRead());
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
    protected Map.Entry<Struct, Struct> nextRecord() {
        if (!hasNext())
            throw new NoSuchElementException("There are no more records in file: " + getFilePath());

        Struct valueStruct = new Struct(valueSchema);

        for (int i = 0; i < valueSchema.fields().size(); i++) {
            Field field = valueSchema.fields().get(i);

            if (null != field) {
                String fieldValue = currentLine[i];
                valueStruct.put(field, fieldValue);
            }
        }

        currentLine = null;

        Struct keyStruct = constructKeySchema(source, name, cobDate, "csv");
        if(isBeginning())
            keyStruct = makeFirst(keyStruct);
        if(!hasNext())
            keyStruct = makeLast(keyStruct);

        return new AbstractMap.SimpleEntry<>(keyStruct, valueStruct);
    }

    @Override
    public void seek(Long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("Record offset must be greater than 0");
        }
        try {
            if (offset < reader.getLinesRead()) {
                InputStream inputStream = getInputStream(fs, getFilePath());
                CSVParser csvParser = CsvSchemaGenerator.createCSVParserBuilder().build();
                this.reader = CsvSchemaGenerator.createCSVReaderBuilder(new InputStreamReader(inputStream, Charset.defaultCharset()), csvParser).build();

                currentLine = null;
            }
            while ((currentLine = reader.readNext()) != null) {
                if (reader.getLinesRead() - 1 == offset) {
                    this.offset.setOffset(reader.getLinesRead());
                    return;
                }
            }
            this.offset.setOffset(reader.getLinesRead());
        } catch (IOException ioe) {
            throw new ConnectException("Error seeking file " + getFilePath(), ioe);
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    private String[] process(String[] input) {
        if(input != null && input.length > valueSchema.fields().size()) {
            String[] line = new String[valueSchema.fields().size()];
            int counter = 0;

            StringBuilder field = new StringBuilder();
            boolean appending = false;
            for(int i = 0; i < input.length; i++) {
                field.append(input[i]);

                if(input[i].startsWith("\"")) {
                    appending = true;
                }

                if(input[i].endsWith("\"")) {
                    appending = false;
                }

                if(appending) {
                    field.append(",");
                }

                if(!appending) {
                    line[counter] = field.toString();
                    field = new StringBuilder();
                    counter += 1;

                }
            }
            return line;
        }

        return input;
    }
}