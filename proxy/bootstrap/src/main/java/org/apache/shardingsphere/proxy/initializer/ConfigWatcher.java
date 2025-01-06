package org.apache.shardingsphere.proxy.initializer;

import org.apache.shardingsphere.proxy.backend.config.YamlProxyConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

public class ConfigWatcher {

    private static final Logger log = LoggerFactory.getLogger(ConfigWatcher.class);
    private final String configUrl;
    private final BootstrapInitializer bootstrapInitializer;
    private YamlProxyConfiguration lastConfig;

    public ConfigWatcher(String configUrl, BootstrapInitializer bootstrapInitializer) {
        this.configUrl = configUrl;
        this.bootstrapInitializer = bootstrapInitializer;
    }

    public void startWatching(long interval) {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    YamlProxyConfiguration newConfig = loadConfigFromHttp(configUrl);
                    if (!newConfig.equals(lastConfig)) {
                        System.out.println("Configuration changed. Reloading...");
                        bootstrapInitializer.init(newConfig, 3307, true);
                        lastConfig = newConfig;
                    }
                } catch (IOException e) {
                    System.err.println("Failed to fetch config: " + e.getMessage());
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }, 0, interval);
    }
    public void printTimer(long interval) {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                    // 获取当前时间并格式化
                    LocalDateTime currentTime = LocalDateTime.now();
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    String formattedTime = currentTime.format(formatter);
                    log.info("Current Time: " + formattedTime + ". Configuration changed. Reloading...");
            }
        }, 0, interval);
    }

    private static YamlProxyConfiguration loadConfigFromHttp(String configUrl) throws IOException {
        String yamlConfig = ConfigFetcher.fetchConfig(configUrl);
        return ConfigParser.parseConfig(yamlConfig);
    }
}
