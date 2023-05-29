package io.siddhi.extension.io.live.utils;

import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class LiveExtensionConfig {
    private final Logger logger = LoggerFactory.getLogger(LiveExtensionConfig.class);
    private final String CONFIG_DIR = System.getProperty("user.dir") + "/src/main/resources";
    private final String FILE_NAME = "LiveExtension.config";
    private final Properties prop;

    private LiveExtensionConfig(final String configFileDir, final String configFileName) throws IOException {
        this.prop = new Properties();
        try (FileInputStream fis = new FileInputStream((configFileDir == null ? CONFIG_DIR : configFileDir) + "/" + (configFileName == null ? FILE_NAME : configFileName))) {
            prop.load(fis);
        }
        logger.info(prop.toString());
    }

    public static class LiveExtensionConfigBuilder {
        private String configFileDir;
        private String configFileName;

        public LiveExtensionConfigBuilder setConfigFileDir(String configFileDir) {
            this.configFileDir = configFileDir;
            return this;
        }

        public LiveExtensionConfigBuilder setConfigFileName(String configFileName) {
            this.configFileName = configFileName;
            return this;
        }

        @SneakyThrows
        public LiveExtensionConfig build() {
            return new LiveExtensionConfig(configFileDir, configFileName);
        }
    }

    public final String getProperty(String key){
        return prop.getProperty(key);
    }

}
