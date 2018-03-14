package com.github.nowackia.kafka.connect.config;

import com.github.nowackia.kafka.connect.policy.AbstractPolicy;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Config for file source connector
 */
public class FsSourceConnectorConfig extends AbstractConfig {

    public static final String ZOOKEEPER_SERVERS = "zookeeper.servers";
    private static final String ZOOKEEPER_SERVERS_DOC = "List of zookeeper servers.";

    public static final String FS_URI = "fs.uri";
    private static final String FS_URI_DOC = "URI of the local file system.";

    public static final String HDFS_URI = "hdfs.uri";
    private static final String HDFS_URI_DOC = "URI of the hdfs file system.";

    public static final String HDFS_LOCATION_FS = "hdfs.fs.location";
    private static final String HDFS_LOCATION_FS_DOC = "Hadoop HDFS server location.";

    public static final String TOPIC = "topic";
    private static final String TOPIC_DOC = "Topic to copy data to.";

    public static final String HDFS_PRINCIPAL = "hdfs.principal";
    private static final String HDFS_PRINCIPAL_DOC = "Principal location for kerberos authentication.";

    public static final String HDFS_KEYTAB = "hdfs.keytab";
    private static final String HDFS_KEYTAB_DOC = "Keytab location for kerberos authentication.";

    public static final String KERBEROS_REFRESH = "kerberos.refresh";
    private static final String KERBEROS_REFRESH_DOC = "Time in hours between kerberos principle refresh.";


    public FsSourceConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public FsSourceConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(FS_URI, Type.STRING, null, Importance.HIGH, FS_URI_DOC)
                .define(HDFS_URI, Type.STRING, null, Importance.HIGH, HDFS_URI_DOC)
                .define(HDFS_LOCATION_FS, Type.STRING, null, Importance.HIGH, HDFS_LOCATION_FS_DOC)
                .define(TOPIC, Type.STRING, Importance.HIGH, TOPIC_DOC)
                .define(ZOOKEEPER_SERVERS, Type.STRING, null, Importance.HIGH, ZOOKEEPER_SERVERS_DOC)
                .define(AbstractPolicy.POLICY_FILTER, Type.STRING, "", Importance.HIGH, TOPIC_DOC)
                .define(HDFS_PRINCIPAL, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, HDFS_PRINCIPAL_DOC)
                .define(HDFS_KEYTAB , ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, HDFS_KEYTAB_DOC)
                .define(KERBEROS_REFRESH , Type.INT, 1, ConfigDef.Importance.LOW, KERBEROS_REFRESH_DOC);
    }

    public String getHdfsFs() {
        return this.getString(HDFS_LOCATION_FS);
    }

    public String getHdfsUri() {
        return this.getString(HDFS_URI);
    }

    public String getFsUri() {
        return this.getString(FS_URI);
    }

    public String getTopic() {
        return this.getString(TOPIC);
    }

    public String getZookeeper() {
        return this.getString(ZOOKEEPER_SERVERS);
    }

    public String getHdfsPrincipal() {
        return this.getString(HDFS_PRINCIPAL);
    }

    public String getHdfsKeytab() {
        return this.getString(HDFS_KEYTAB);
    }

    public int getKerberosRefresh() {
        return this.getInt(KERBEROS_REFRESH);
    }

    public List<String> getPolicyFilters() {
        String[] filters = this.getString(AbstractPolicy.POLICY_FILTER).split(",");
        return Arrays.asList(filters);
    }

}