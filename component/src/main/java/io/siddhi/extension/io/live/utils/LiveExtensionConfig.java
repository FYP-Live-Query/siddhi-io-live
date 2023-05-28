package io.siddhi.extension.io.live.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class LiveExtensionConfig {
    private final String CONFIG_DIR = "component/src/main/resources";
    private final String FILE_NAME = "LiveExtension.config";
    private final Properties prop;

    public LiveExtensionConfig(final String configFileDir, final String configFileName) throws IOException {
        this.prop = new Properties();
        try (FileInputStream fis = new FileInputStream(
                (configFileDir == null ? CONFIG_DIR : configFileDir) + "/" + (configFileName == null ? FILE_NAME : configFileName)
        )) {
            prop.load(fis);
        }
    }

    public final String getProperty(String key){
        return prop.getProperty(key);
    }

}
