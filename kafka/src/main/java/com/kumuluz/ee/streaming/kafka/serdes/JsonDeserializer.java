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
package com.kumuluz.ee.streaming.kafka.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Json deserializer for Kafka.
 *
 * @author Urban Malc
 * @since 1.2.0
 */
public class JsonDeserializer<T> implements Deserializer<T> {

    private static final Logger LOG = Logger.getLogger(JsonDeserializer.class.getName());

    private ObjectMapper objectMapper;
    private Class<T> type;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.objectMapper = new ObjectMapper();

        String fineType = (isKey) ? "key.deserializer.type" : "value.deserializer.type";

        this.type = parseType((String) configs.get(fineType));

        if (this.type == null) {
            this.type = parseType((String) configs.get("deserializer.type"));
        }

        if (this.type == null) {
            throw new ConfigException("JsonDeserializer type not found. Please specify its type in the configuration.");
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {

        if (this.type == null) {
            throw new SerializationException("Could not deserialize data because deserialization JSON type was not " +
                    "properly defined.");
        }

        if (data == null || data.length == 0) {
            return null;
        }

        try {
            return objectMapper.readValue(data, this.type);
        } catch (IOException e) {
            throw new SerializationException("Could not deserialize JSON", e);
        }
    }

    @Override
    public void close() {

    }

    @SuppressWarnings("unchecked")
    private Class<T> parseType(String klass) {
        if (klass == null) {
            return null;
        }

        try {
            return (Class<T>) Class.forName(klass);
        } catch (ClassNotFoundException e) {
            LOG.warning("Class " + klass + " not found on the classpath");
            return null;
        }
    }
}
