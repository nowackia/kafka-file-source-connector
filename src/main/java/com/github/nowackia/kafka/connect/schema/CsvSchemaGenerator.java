package com.github.nowackia.kafka.connect.schema;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema generator for csv files
 */
public class CsvSchemaGenerator {

    private static final Logger log = LoggerFactory.getLogger(CsvSchemaGenerator.class);

    private final String fileName;
    private final InputStream stream;

    public CsvSchemaGenerator(String path, InputStream stream) throws IOException {
        this.fileName = Paths.get(path).getFileName().toString();
        this.stream = stream;
    }

    public Schema generate() throws IOException {

        SchemaBuilder valueSchemaBuilder = SchemaBuilder.struct().name(fileName + ".value");

        Map<String, Schema.Type> fieldTypes = determineFieldTypes(stream);
        for (Map.Entry<String, Schema.Type> kvp : fieldTypes.entrySet()) {
            addField(valueSchemaBuilder, kvp.getKey(), kvp.getValue());
        }

        return valueSchemaBuilder.build();
    }

    protected void addField(SchemaBuilder builder, String name, Schema.Type schemaType) {
        log.trace("addField() - name = {} schemaType = {}", name, schemaType);
        builder.field(name, SchemaBuilder.type(schemaType).optional().build());
    }

    protected Map<String, Schema.Type> determineFieldTypes(InputStream stream) throws IOException {
        Map<String, Schema.Type> typeMap = new LinkedHashMap<>();

        CSVParser csvParser = CsvSchemaGenerator.createCSVParserBuilder().build();
        try (CSVReader csvReader = new CSVReader(new InputStreamReader(stream), 0, csvParser)) {
            String[] headers = csvReader.readNext();

            for (String s : headers) {
                typeMap.put(s, Schema.Type.STRING);
            }
        }
        return typeMap;
    }

    public static CSVParserBuilder createCSVParserBuilder() {
        return new CSVParserBuilder()
                .withEscapeChar(CSVParser.DEFAULT_ESCAPE_CHARACTER)
                .withIgnoreLeadingWhiteSpace(CSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE)
                .withIgnoreQuotations(CSVParser.DEFAULT_IGNORE_QUOTATIONS)
                .withQuoteChar('\u0000')
                .withSeparator(CSVParser.DEFAULT_SEPARATOR)
                .withStrictQuotes(CSVParser.DEFAULT_STRICT_QUOTES)
                .withFieldAsNull(CSVParser.DEFAULT_NULL_FIELD_INDICATOR);
    }

    public static CSVReaderBuilder createCSVReaderBuilder(Reader reader, CSVParser parser) {
        return new CSVReaderBuilder(reader)
                .withCSVParser(parser)
                .withKeepCarriageReturn(CSVReader.DEFAULT_KEEP_CR)
                .withSkipLines(1)
                .withVerifyReader(CSVReader.DEFAULT_VERIFY_READER)
                .withFieldAsNull(CSVParser.DEFAULT_NULL_FIELD_INDICATOR);
    }


}
