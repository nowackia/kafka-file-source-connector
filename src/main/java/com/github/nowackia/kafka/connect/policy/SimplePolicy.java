package com.github.nowackia.kafka.connect.policy;

import com.github.nowackia.kafka.connect.config.FsSourceTaskConfig;
import com.github.nowackia.kafka.connect.file.FileMetadata;
import com.github.nowackia.kafka.connect.file.reader.FileReader;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Simple policy for file reading with a simple sleep in between checks
 */
public class SimplePolicy extends AbstractPolicy {

    private static final Logger log = LoggerFactory.getLogger(SimplePolicy.class);

    private static final int DEFAULT_SLEEP_FRACTION = 10;

    public static final String SLEEPY_POLICY_SLEEP_MS = POLICY_PREFIX + "sleep";
    public static final String SLEEPY_POLICY_SLEEP_FRACTION = POLICY_PREFIX + "fraction";

    private boolean firstRun = true;

    private long sleep;
    private long sleepFraction;

    public SimplePolicy(FsSourceTaskConfig conf) throws IOException {
        super(conf);
    }

    @Override
    protected void configPolicy(Map<String, Object> customConfigs) {
        super.configPolicy(customConfigs);

        try {
            this.sleep = Long.valueOf((String) customConfigs.get(SLEEPY_POLICY_SLEEP_MS));
        } catch (NumberFormatException nfe) {
            throw new ConfigException(SLEEPY_POLICY_SLEEP_MS + " property is required and must be a number(long). Got: " +
                    customConfigs.get(SLEEPY_POLICY_SLEEP_MS));
        }
        if (customConfigs.get(SLEEPY_POLICY_SLEEP_FRACTION) != null) {
            try {
                this.sleepFraction = Long.valueOf((String) customConfigs.get(SLEEPY_POLICY_SLEEP_FRACTION));
            } catch (NumberFormatException nfe) {
                throw new ConfigException(SLEEPY_POLICY_SLEEP_FRACTION + " property must be a number(long). Got: " +
                        customConfigs.get(SLEEPY_POLICY_SLEEP_FRACTION));
            }
        } else {
            this.sleepFraction = DEFAULT_SLEEP_FRACTION;
        }
    }

    @Override
    protected void postCheck() {
        if(filters != null) {
            Iterator<Map.Entry<FileMetadata, FileReader>> iter = this.files.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<FileMetadata, FileReader> entry = iter.next();

                boolean matches = false;
                for(String filter : filters) {
                    Pattern p = Pattern.compile(filter);
                    Matcher m = p.matcher(Paths.get(entry.getValue().getFilePath()).getFileName().toString());
                    if (m.matches()) {
                        matches = true;
                        break;
                    }
                }

                if(!matches)
                    iter.remove();
            }
        }

        sleepIfApply();
    }

    private void sleepIfApply() {
        if (getExecutions() > 0) {
            if(firstRun) {
                firstRun = false;
                resetExecutions();
            } else {
                int counter = 0;
                while (!hasEnded() && counter < sleepFraction) {
                    try {
                        long sleepMillis = sleep / sleepFraction;
                        log.debug("Sleeping for " + sleepMillis + "ms prior to another check");
                        Thread.sleep(sleepMillis);
                        counter++;
                        resetExecutions();
                    } catch (InterruptedException ie) {
                        log.warn("An interrupted exception has occurred.", ie);
                    }
                }
            }
        }
    }

    @Override
    protected boolean isPolicyCompleted() {
        return false;
    }

}
