package com.kumuluz.ee.streaming.kafka.config;

import com.kumuluz.ee.streaming.common.config.ConfigLoader;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Map;

/**
 * @author Matija Kljun
 */
public class KafkaStreamsConfigLoader implements ConfigLoader {

    private final static String CONFIG_PREFIX = "kumuluzee.streaming.kafka";

    public static Map<String, Object> getConfig(String configName) {
        return ConfigLoader.getConfig(StreamsConfig.configDef().names().iterator(), CONFIG_PREFIX + "." + configName);
    }

}
