package com.github.nowackia.kafka.connect.policy;

import com.github.nowackia.kafka.connect.config.FsSourceTaskConfig;
import com.github.nowackia.kafka.connect.file.FileMetadata;
import com.github.nowackia.kafka.connect.file.reader.BinaryFileReader;
import com.github.nowackia.kafka.connect.file.reader.FileReader;
import com.github.nowackia.kafka.connect.file.reader.CsvFileReader;
import com.github.nowackia.kafka.connect.file.reader.TextFileReader;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.github.nowackia.kafka.connect.config.FsSourceConnectorConfig.HDFS_KEYTAB;
import static com.github.nowackia.kafka.connect.config.FsSourceConnectorConfig.HDFS_PRINCIPAL;
import static org.apache.hadoop.security.SecurityUtil.login;

/**
 * Base policy for file reading
 */
public abstract class AbstractPolicy implements Policy {

    private final Logger log = LoggerFactory.getLogger(getClass());

    protected static final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyyMMdd");
    protected static final String POLICY_PREFIX = FsSourceTaskConfig.POLICY_PREFIX;

    public static final String POLICY_FILTER = POLICY_PREFIX + "filter";
    public static final String POLICY_RECURSIVE = POLICY_PREFIX + "recursive";

    protected final String filePath;

    protected Map<FileMetadata, FileReader> files;

    private final FsSourceTaskConfig conf;
    private final AtomicInteger executions;
    private boolean interrupted;

    private FileSystem fs;

    private String currentCobDate;

    protected List<String> filters;
    protected boolean recursive;

    public AbstractPolicy(FsSourceTaskConfig conf) throws IOException {
        this.conf = conf;
        this.executions = new AtomicInteger(1);
        this.interrupted = false;
        this.files = new HashMap<>();
        this.currentCobDate = null;
        this.recursive = false;

        Map<String, Object> customConfigs = customConfigs();
        configPolicy(customConfigs);

        if(conf.getHdfsFs() != null && conf.getHdfsUri() != null) {
            this.filePath = conf.getHdfsUri();

            Configuration fsConfig = new Configuration();
            fsConfig.set("fs.defaultFS", conf.getHdfsFs());
            fsConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            fsConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            fs = FileSystem.get(fsConfig);

            boolean isKerberosEnabled = conf.getHdfsPrincipal() != null && conf.getHdfsKeytab() != null;
            if(isKerberosEnabled) {
                Configuration kerberosConf = new Configuration();
                kerberosConf.set(HDFS_PRINCIPAL, conf.getHdfsPrincipal());
                kerberosConf.set(HDFS_KEYTAB, conf.getHdfsKeytab());
                ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
                service.scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            log.info("Authenticating using kerberos principal: {} and keytab: {}", conf.getHdfsPrincipal(), conf.getHdfsKeytab());
                            login(kerberosConf, HDFS_KEYTAB, HDFS_PRINCIPAL);
                        } catch (IOException e) {
                            log.error("Error in refreshing login", e);
                        }
                    }
                }, 0, conf.getKerberosRefresh(), TimeUnit.HOURS);
            }
        }
        else {
            this.filePath = this.conf.getFsUri();
            fs = null;
        }
    }

    private Map<String, Object> customConfigs() {
        return conf.originals().entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
    }

    protected void configPolicy(Map<String, Object> customConfigs) {
        this.filters = null;
        if (customConfigs.get(POLICY_FILTER) != null) {
            String[] policies = ((String) customConfigs.get(POLICY_FILTER)).split(",");
            if(policies.length > 0) {
                this.filters = new ArrayList<>();
                for (String filter : policies)
                    this.filters.add(filter.trim());
            }
        }
        if (customConfigs.get(POLICY_RECURSIVE) != null) {
            recursive = Boolean.valueOf(customConfigs.get(POLICY_RECURSIVE).toString());
        }
    }

    @Override
    public List<String> getFilters() {
        return this.filters;
    }

    @Override
    public String getURI() {
        return filePath;
    }

    @Override
    public final Set<FileMetadata> execute(OffsetStorageReader offsetStorageReader) throws IOException {
        if (hasEnded()) {
            throw new IllegalWorkerStateException("Policy has ended. Cannot be retried");
        }
        preCheck();

        if(getExecutions() > 0)
            files = concatFiles(filePath, files, offsetStorageReader);

        postCheck();

        return files.keySet();
    }

    @Override
    public void interrupt() {
        interrupted = true;
    }

    protected void preCheck() {
    }

    protected void postCheck() {
    }

    public Map<FileMetadata, FileReader> concatFiles(String path, Map<FileMetadata, FileReader> files, OffsetStorageReader offsetStorageReader) throws IOException {
        String cobDate = this.conf.getCobDate();
        if(cobDate == null) {
            LocalDate date = new LocalDate();

            /* Handle using previous business day for path creation */
            if(path.contains("{yyyyMMdd-1}")) {
                boolean businessDay = false;
                while (!businessDay) {
                    date = date.minusDays(1);
                    businessDay = (date.getDayOfWeek() <= 5);
                }
                path = path.replace("{yyyyMMdd-1}", "{yyyyMMdd}");
            }
            cobDate = formatter.print(date);
        }

        if(cobDate != currentCobDate) {
            currentCobDate = cobDate;
            if(conf.getLogPollingMessage())
                log.info("Searching directory for cob-date {}", currentCobDate);

            Iterator<FileReader> readers = files.values().iterator();
            while(readers.hasNext()) {
                FileReader reader = readers.next();

                if(!reader.getCobDate().equals(this.currentCobDate) && reader.isFinished()) {
                    log.info("Closing reader on {}", reader.getFilePath());
                    reader.close();
                    readers.remove();
                }
            }
        }

        path = path.replace("{yyyyMMdd}", cobDate);
        List<FileMetadata> collection = new ArrayList<>();
        if(fs == null) {
            File directory = new File(path);
            if (directory.exists()) {
                Collection<File> list = FileUtils.listFiles(directory, null, false);

                for (File file : list)
                    collection.add(toMetadata(file));
            }
            else {
                if(conf.getLogPollingMessage())
                    log.info("Directory {} does not exist yet", path.toString());
            }
        } else {
            org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(path);
            if(fs.exists(hdfsPath)) {
                RemoteIterator<LocatedFileStatus> statuses = fs.listFiles(new org.apache.hadoop.fs.Path(path), recursive);
                while (statuses.hasNext()) {
                    LocatedFileStatus fileStatus = statuses.next();
                    collection.add(toMetadata(fileStatus));
                }
            } else {
                if(conf.getLogPollingMessage())
                    log.info("HDFS location {} does not exist yet", path.toString());
            }
        }

        for (FileMetadata latest : collection) {
            boolean create = true;
            boolean reset = false;
            for(FileMetadata metadata : files.keySet()) {
                if(metadata.getPath().equals(latest.getPath())) {
                    create = false;

                    if(metadata.getLen() != latest.getLen()) {
                        create = true;
                        reset = true;
                    }
                }
            }

            if(create) {
                Map<String, Object> partition = new HashMap<String, Object>() {{
                    put("path", latest.getPath());
                }};

                FileReader reader;
                try {
                    reader = getReader(this.fs, latest, cobDate, conf);
                } catch (Throwable t) {
                    throw new ConnectException("An error has occurred when creating reader for file: " + latest.getPath(), t);
                }

                if(!reset) {
                    Map<String, Object> offset = offsetStorageReader.offset(partition);
                    if (offset != null && offset.get("offset") != null) {
                        reader.seek((Long)offset.get("offset"));
                    }
                }

                files.put(latest, reader);
            }
        }

        return files;
    }

    private FileReader getReader(FileSystem fs, FileMetadata latest, String cobDate, FsSourceTaskConfig conf) throws IOException {
        Path path = Paths.get(latest.getPath());
        String extension = FilenameUtils.getExtension(path.getFileName().toString());

        FileReader reader = null;
        if(extension.equalsIgnoreCase("csv"))
            reader = new CsvFileReader(fs, latest, cobDate, conf);
        else if(extension.equalsIgnoreCase("txt") || extension.equalsIgnoreCase("json"))
            reader = new TextFileReader(fs, latest, cobDate, conf);
        else if(extension.equalsIgnoreCase("gz") || extension.equalsIgnoreCase("zip"))
            reader = new BinaryFileReader(fs, latest, cobDate, conf);
        else {
            if(conf.getLogPollingMessage())
                log.warn("No default FileReader defined for {}, using BinaryFileReader", path.getFileName().toString());

            reader = new BinaryFileReader(fs, latest, cobDate, conf);
        }

        return reader;
    }


    @Override
    public final boolean hasEnded() {
        return interrupted || isPolicyCompleted();
    }

    protected abstract boolean isPolicyCompleted();

    public final int getExecutions() {
        return executions.get();
    }

    public final void resetExecutions() {
        executions.set(0);
    }

    public final void incrementExecutions() {
        executions.incrementAndGet();
    }

    protected FileMetadata toMetadata(File fileStatus) {
        return new FileMetadata(fileStatus.getPath().toString(), fileStatus.length());
    }

    protected FileMetadata toMetadata(FileStatus fileStatus) {
        return new FileMetadata(fileStatus.getPath().toString(), fileStatus.getLen());
    }

    @Override
    public FileReader offer(FileMetadata metadata, OffsetStorageReader offsetStorageReader) throws IOException {
        return files.get(metadata);
    }

    @Override
    public void close() throws IOException {
        this.interrupt();

        if(fs != null)
            fs.close();

        Iterator<FileReader> iterator = files.values().iterator();
        while (iterator.hasNext()) {
            iterator.next().close();
            iterator.remove();
        }
    }
}