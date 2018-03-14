package com.github.nowackia.kafka.connect;

import com.github.nowackia.kafka.connect.config.FsSourceTaskConfig;
import com.github.nowackia.kafka.connect.file.FileMetadata;
import com.github.nowackia.kafka.connect.file.Offset;
import com.github.nowackia.kafka.connect.file.reader.FileReader;
import com.github.nowackia.kafka.connect.policy.Policy;
import com.github.nowackia.kafka.connect.policy.SimplePolicy;
import com.github.nowackia.kafka.connect.util.Version;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.nowackia.kafka.connect.config.FsSourceConnectorConfig.HDFS_KEYTAB;
import static com.github.nowackia.kafka.connect.config.FsSourceConnectorConfig.HDFS_PRINCIPAL;
import static org.apache.hadoop.security.SecurityUtil.login;

/**
 * Task with the file source logic (coming from both local drive and hdfs)
 */
public class FsSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(FsSourceTask.class);

    private AtomicBoolean stop;
    private FsSourceTaskConfig config;
    private Policy policy;

    @Override
    public String version() {
        return Version.CURRENT.toString();
    }

    @Override
    public void start(Map<String, String> properties) {
        try {
            config = new FsSourceTaskConfig(properties);
            policy = new SimplePolicy(config);
            log.info("Assigned policy file filer: {}", policy.getFilters());

            boolean isKerberosEnabled = config.getHdfsPrincipal() != null && config.getHdfsKeytab() != null;
            if(isKerberosEnabled) {
                Configuration kerberosConf = new Configuration();
                kerberosConf.set(HDFS_PRINCIPAL, config.getHdfsPrincipal());
                kerberosConf.set(HDFS_KEYTAB, config.getHdfsKeytab());
                ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
                service.scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            log.info("Authenticating using kerberos principal: {} and keytab: {}", config.getHdfsPrincipal(), config.getHdfsKeytab());
                            login(kerberosConf, HDFS_KEYTAB, HDFS_PRINCIPAL);
                        } catch (IOException e) {
                            log.error("Error in refreshing login", e);
                        }
                    }
                }, 0, config.getKerberosRefresh(), TimeUnit.HOURS);
            }

        } catch (ConfigException ce) {
            log.error("Couldn't start FsSourceTask:", ce);
            throw new ConnectException("Couldn't start FsSourceTask due to configuration error", ce);
        } catch (Throwable t) {
            log.error("Couldn't start FsSourceConnector:", t);
            throw new ConnectException("A problem has occurred reading configuration:" + t.getMessage());
        }

        stop = new AtomicBoolean(false);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        while (stop != null && !stop.get() && !policy.hasEnded()) {

            final List<SourceRecord> results = new ArrayList<>();

            Set<FileMetadata> files = filesToProcess();

            int maxLinesPerRecord = this.config.getMaxLinesPerRecord();
            int maxRecordsPerPublish = this.config.getMaxRecordsPerPublish();
            AtomicInteger processed = new AtomicInteger(0);

            for(FileMetadata metadata : files) {
                try {
                    FileReader reader = policy.offer(metadata, context.offsetStorageReader());
                    if(!reader.isFinished()) {

                        if(reader.isBeginning())
                            log.info("Started processing file {}", metadata);

                        while(reader.hasNext() && results.size() < maxRecordsPerPublish) {
                            ArrayList<Struct> valueArray = new ArrayList<>();
                            Map.Entry<Struct, Struct> next = null;
                            Struct keyStruct = null;
                            while (reader.hasNext() && valueArray.size() < maxLinesPerRecord) {
                                next = reader.next();
                                valueArray.add(next.getValue());
                                keyStruct = combine(keyStruct, next.getKey());
                                reader.logProgress(log, config.getLogPercentageInterval());
                            }
                            Schema valueArraySchema = SchemaBuilder.array(next.getValue().schema()).name(next.getValue().schema().name()).build();
                            results.add(convert(metadata, reader.currentOffset(), keyStruct, valueArraySchema, valueArray));
                        }

                        if(!reader.hasNext()) {
                            log.info("Finished processing file {}", metadata);
                            processed.incrementAndGet();
                            continue;
                        }

                        if (results.size() >= maxRecordsPerPublish)
                            break;
                    } else
                        processed.incrementAndGet();

                } catch (ConnectException | IOException e) {
                    log.error("Error reading file from FS: " + metadata.getPath() + ". Keep going...", e);
                }
            }

            if(processed.get() >= files.size()) {
                policy.incrementExecutions();
                if(config.getLogPollingMessage())
                    log.info("[" + this.config.getName() + " - partition: " + config.getPartition()  + "] Polled all files, sleeping ...");
            }

            return results;
        }

        return null;
    }

    private Set<FileMetadata> filesToProcess() {
        try {
            Set<FileMetadata> list = policy.execute(context.offsetStorageReader());
            return list;
        } catch (IOException | ConnectException e) {
            log.error("Cannot retrieve files to process from FS: " + policy.getURI() + ". Keep going...", e);
            return Collections.EMPTY_SET;
        }
    }

    private Struct combine(Struct original, Struct add) {
        if(original == null)
            return add;

        String firstName = original.schema().fields().get(5).name();
        String lastName = original.schema().fields().get(6).name();

        original.put(firstName, original.getBoolean(firstName) || add.getBoolean(firstName));
        original.put(lastName, original.getBoolean(lastName) || add.getBoolean(lastName));

        return original;
    }

    private SourceRecord convert(FileMetadata metadata, Offset offset, Struct keyStruct, Schema valueSchema, ArrayList<Struct> valueArray) {

        Map<String, Object> sourcePartition = new HashMap<String, Object>() {
            { put("path", metadata.getPath()); }
        };

        Map<String, Object> sourceOffset = new HashMap<String, Object>() {
            { put("offset", offset.getRecordOffset()); }
        };

        SourceRecord record = new SourceRecord(sourcePartition, sourceOffset, config.getTopic(), config.getPartition(),
                keyStruct.schema(), keyStruct, valueSchema, valueArray);

        return record;
    }

    @Override
    public void stop() {
        try {
            if (stop != null) {
                stop.set(true);
            }
            if (policy != null) {
                policy.close();
            }
        } catch(Exception e) {
            log.error("Issue stopping task: ", e);
        }
    }
}
