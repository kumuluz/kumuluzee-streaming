package com.kumuluz.ee.streaming.kafka.config;

import com.kumuluz.ee.streaming.common.annotations.ConfigurationOverride;
import com.kumuluz.ee.streaming.common.config.ConfigLoader;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Map;

/**
 * Config loader for Kafka streams.
 *
 * @author Matija Kljun
 * @since 1.0.0
 */
public class KafkaStreamsConfigLoader {

    private final static String CONFIG_PREFIX = "kumuluzee.streaming.kafka";

    public static Map<String, Object> getConfig(String configName, ConfigurationOverride[] overrides) {
        return ConfigLoader.getConfig(StreamsConfig.configDef().names().iterator(),
                CONFIG_PREFIX + "." + configName,
                overrides);
    }

}
