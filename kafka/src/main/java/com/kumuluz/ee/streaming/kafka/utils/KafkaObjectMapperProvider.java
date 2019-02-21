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
package com.kumuluz.ee.streaming.kafka.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Provides {@link ObjectMapper} for the JSON Serializer/Deserializer/SerDe.
 *
 * @author Urban Malc
 * @since 1.2.0
 */
public interface KafkaObjectMapperProvider {

    /**
     * Supplies the {@link ObjectMapper} for use in JSON Serializer/Deserializer/SerDe. If method returns null, this
     * provider is ignored. If no providers are found, the default {@link ObjectMapper} is used.
     *
     * @param configs kafka configuration for JSON Serializer/Deserializer/SerDe
     * @param isKey is JSON Serializer/Deserializer/SerDe for key
     * @return mapper that should be used for this configuration or null if not applicable
     */
    ObjectMapper provideObjectMapper(Map<String, ?> configs, boolean isKey);

    static ObjectMapper getObjectMapper(Map<String, ?> configs, boolean isKey) {

        List<ObjectMapper> objectMappers = new ArrayList<>();

        ServiceLoader.load(KafkaObjectMapperProvider.class).forEach(provider -> {
            ObjectMapper om = provider.provideObjectMapper(configs, isKey);
            if (om != null) {
                objectMappers.add(om);
            }
        });

        if (objectMappers.size() == 0) {
            return new ObjectMapper();
        } else if (objectMappers.size() == 1) {
            return objectMappers.get(0);
        } else {
            throw new ConfigException("Multiple ObjectMapper providers found.");
        }
    }
}
