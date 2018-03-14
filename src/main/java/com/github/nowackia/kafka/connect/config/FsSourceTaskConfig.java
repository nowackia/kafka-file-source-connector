package com.github.nowackia.kafka.connect.config;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * Config for file source task
 */
public class FsSourceTaskConfig extends FsSourceConnectorConfig {

    public static final String POLICY_PREFIX = "policy.";

    public static final String NAME = "name";
    private static final String NAME_DOC = "Connector's name.";

    public static final String PARTITION = "partition.id";
    private static final String PARTITION_DOC = "Number of partition to publish to.";

    public static final String MAX_LINES_PER_SOURCE_RECORDS = "max.lines.records";
    private static final String MAX_LINES_PER_SOURCE_RECORDS_DOC = "Max lines to be read and included in a record.";

    public static final String MAX_SOURCE_RECORDS_PER_PUBLISH = "max.source.records";
    private static final String MAX_SOURCE_RECORDS_PER_PUBLISH_DOC = "Max source records to be published in during one poll.";

    public static final String LOG_PERCENTAGE_INTERVAL = "logging.percentage.interval";
    private static final String LOG_PERCENTAGE_INTERVAL_DOC = "Interval in percentage [0 ... 100%] at which logging file processing progress happens.";

    public static final String LOG_POLLING_MESSAGE = "logging.polling.message";
    private static final String LOG_POLLING_MESSAGE_DOC = "Enable logging polling files message at regular intervals.";

    public static final String COB_DATE = "cob.date";
    private static final String COB_DATE_DOC = "COB date to overwrite running with.";

    public static final String SOURCE = "source";
    private static final String SOURCE_DOC = "Source description of the files.";

    public FsSourceTaskConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public FsSourceTaskConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return FsSourceConnectorConfig.conf()
                .define(NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, NAME_DOC)
                .define(MAX_LINES_PER_SOURCE_RECORDS, ConfigDef.Type.INT, 1000, ConfigDef.Importance.HIGH, MAX_LINES_PER_SOURCE_RECORDS_DOC)
                .define(MAX_SOURCE_RECORDS_PER_PUBLISH, ConfigDef.Type.INT,1, ConfigDef.Importance.HIGH, MAX_SOURCE_RECORDS_PER_PUBLISH_DOC)
                .define(LOG_PERCENTAGE_INTERVAL, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, LOG_PERCENTAGE_INTERVAL_DOC)
                .define(LOG_POLLING_MESSAGE, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH, LOG_POLLING_MESSAGE_DOC)
                .define(COB_DATE, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, COB_DATE_DOC)
                .define(SOURCE, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, SOURCE_DOC)
                .define(PARTITION, ConfigDef.Type.INT, 1, ConfigDef.Importance.HIGH, PARTITION_DOC);
    }

    public String getName() {
        return this.getString(NAME);
    }

    public int getMaxLinesPerRecord() {
        return this.getInt(MAX_LINES_PER_SOURCE_RECORDS);
    }

    public int getMaxRecordsPerPublish() {
        return this.getInt(MAX_SOURCE_RECORDS_PER_PUBLISH);
    }

    public int getLogPercentageInterval() {
        return this.getInt(LOG_PERCENTAGE_INTERVAL);
    }

    public boolean getLogPollingMessage() {
        return this.getBoolean(LOG_POLLING_MESSAGE);
    }

    public String getSource() {
        return this.getString(SOURCE);
    }

    public int getPartition() {
        return this.getInt(PARTITION);
    }

    public String getCobDate() {
        return this.getString(COB_DATE);
    }

}
