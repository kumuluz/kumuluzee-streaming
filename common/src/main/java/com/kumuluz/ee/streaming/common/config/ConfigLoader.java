/*
 *  Copyright (c) 2014-2017 Kumuluz and/or its affiliates
 *  and other contributors as indicated by the @author tags and
 *  the contributor list.
 *
 *  Licensed under the MIT License (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  https://opensource.org/licenses/MIT
 *
 *  The software is provided "AS IS", WITHOUT WARRANTY OF ANY KIND, express or
 *  implied, including but not limited to the warranties of merchantability,
 *  fitness for a particular purpose and noninfringement. in no event shall the
 *  authors or copyright holders be liable for any claim, damages or other
 *  liability, whether in an action of contract, tort or otherwise, arising from,
 *  out of or in connection with the software or the use or other dealings in the
 *  software. See the License for the specific language governing permissions and
 *  limitations under the License.
*/

package com.kumuluz.ee.streaming.common.config;

import com.kumuluz.ee.configuration.utils.ConfigurationUtil;
import com.kumuluz.ee.streaming.common.annotations.ConfigurationOverride;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Utility methods for processing configuration.
 *
 * @author Matija Kljun
 * @since 1.0.0
 */
public class ConfigLoader {

    private static final Logger log = Logger.getLogger(ConfigLoader.class.getName());

    public static Map<String, Object> getConfig(Iterator<String> configProps, String configPrefix,
                                                ConfigurationOverride[] overrides) {
        ConfigurationUtil confUtil = ConfigurationUtil.getInstance();

        Map<String, String> overridesMap = new HashMap<>();

        if (overrides != null) {
            for (ConfigurationOverride override : overrides) {
                overridesMap.put(override.key(), override.value());
            }
        }

        Map<String, Object> prop = new HashMap<>();
        while (configProps.hasNext()) {
            try {
                String configProp = configProps.next();
                String configPropKumuluz = configProp.replace('.', '-');
                String configName = configPrefix + "." + configPropKumuluz;

                if (overridesMap.containsKey(configPropKumuluz)) {
                    prop.put(configProp, overridesMap.get(configPropKumuluz));
                } else if (confUtil.get(configName).isPresent()) {
                    prop.put(configProp, confUtil.get(configName).get());
                }
            } catch (Exception e) {
                log.severe("Unable to read configuration " + configPrefix + ": " + e.toString());
            }
        }
        return prop;
    }

    public static Properties asProperties(Map<String, Object> config) {
        Properties properties = new Properties();

        config.forEach((key, value) -> {
            if (value instanceof String) {
                properties.setProperty(key, (String) value);
            }
        });

        return properties;
    }
}
