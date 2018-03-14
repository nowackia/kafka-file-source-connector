package com.github.nowackia.kafka.connect;

import com.github.nowackia.kafka.connect.config.FsSourceConnectorConfig;
import com.github.nowackia.kafka.connect.config.FsSourceTaskConfig;
import com.github.nowackia.kafka.connect.policy.AbstractPolicy;
import com.github.nowackia.kafka.connect.util.Version;
import kafka.admin.AdminUtils;
import kafka.utils.*;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Connector for file source (coming from both local drive and hdfs)
 */
public class FsSourceConnector extends SourceConnector {

    private final static Logger log = LoggerFactory.getLogger(FsSourceConnector.class);

    private FsSourceConnectorConfig config;

    @Override
    public String version() {
        return Version.CURRENT.toString();
    }

    @Override
    public void start(Map<String, String> properties) {

        log.info("Starting FsSourceConnector...");
        try {
            config = new FsSourceConnectorConfig(properties);

        } catch (ConfigException ce) {
            log.error("Couldn't start FsSourceConnector:", ce);
            throw new ConnectException("Couldn't start FsSourceConnector due to configuration error.", ce);
        } catch (Exception ce) {
            log.error("Couldn't start FsSourceConnector:", ce);
            throw new ConnectException("An error has occurred when starting FsSourceConnector" + ce);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FsSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (config == null) {
            throw new ConnectException("Connector config has not been initialized");
        }
        List<Map<String, String>> taskConfigs = new ArrayList<>();

        int maxPartitions = getPartitionsForTopic(config.getZookeeper(), config.getTopic());

        int groups = Math.max(Math.min(maxPartitions, Math.min(config.getPolicyFilters().size(), maxTasks)), 1);
        ConnectorUtils.groupPartitions(config.getPolicyFilters(), groups)
                .forEach(files -> {
                    Map<String, String> taskProps = new HashMap<>(config.originalsStrings());
                    taskProps.put(AbstractPolicy.POLICY_FILTER, files.stream().collect(Collectors.joining(",")));
                    taskConfigs.add(taskProps);
                });

        int partition = groups - 1;
        for(Map<String, String> taskProps : taskConfigs) {
            taskProps.put(FsSourceTaskConfig.PARTITION, Integer.toString(partition));
            partition -= 1;
        }

        log.info("Starting {} tasks", groups);

        return taskConfigs;
    }

    private int getPartitionsForTopic(String zookeper, String topic) {
        if(zookeper != null) {
            ZkClient zkClient = new ZkClient(zookeper, Integer.MAX_VALUE, 10000, ZKStringSerializer$.MODULE$);
            ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeper), false);

            MetadataResponse.TopicMetadata metaData = AdminUtils.fetchTopicMetadataFromZk(topic, zkUtils);
            return metaData.partitionMetadata().size();
        } else
            return Integer.MAX_VALUE;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return FsSourceConnectorConfig.conf();
    }
}
