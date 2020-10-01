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

import com.kumuluz.ee.streaming.kafka.config.KumuluzEeCustomConfig;
import com.kumuluz.ee.streaming.kafka.config.KumuluzEeCustomStreamsConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Json SerDe for Kafka streams.
 *
 * @author Urban Malc
 * @since 1.2.0
 */
public class JsonSerde<T> implements Serde<T> {

    private Serializer<T> jsonSerializer;
    private Deserializer<T> jsonDeserializer;

    public JsonSerde() {
    }

    public JsonSerde(Class<T> deserializerType) {
        configure(Collections.singletonMap(KumuluzEeCustomStreamsConfig.DEFAULT_SERDE_TYPE,
                deserializerType.getName()), true);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> deserializerConfig = new HashMap<>();
        if (configs.containsKey(KumuluzEeCustomStreamsConfig.DEFAULT_KEY_SERDE_TYPE)) {
            deserializerConfig.put(KumuluzEeCustomConfig.KEY_DESERIALIZER_TYPE,
                    configs.get(KumuluzEeCustomStreamsConfig.DEFAULT_KEY_SERDE_TYPE));
        }
        if (configs.containsKey(KumuluzEeCustomStreamsConfig.DEFAULT_VALUE_SERDE_TYPE)) {
            deserializerConfig.put(KumuluzEeCustomConfig.VALUE_DESERIALIZER_TYPE,
                    configs.get(KumuluzEeCustomStreamsConfig.DEFAULT_VALUE_SERDE_TYPE));
        }
        if (configs.containsKey(KumuluzEeCustomStreamsConfig.DEFAULT_SERDE_TYPE)) {
            deserializerConfig.put(KumuluzEeCustomConfig.DESERIALIZER_TYPE,
                    configs.get(KumuluzEeCustomStreamsConfig.DEFAULT_SERDE_TYPE));
        }
        deserializerConfig.putAll(configs);

        jsonSerializer = new JsonSerializer<>();
        jsonSerializer.configure(configs, isKey);
        jsonDeserializer = new JsonDeserializer<>();
        jsonDeserializer.configure(deserializerConfig, isKey);
    }

    @Override
    public void close() {
        jsonSerializer.close();
        jsonDeserializer.close();
    }

    @Override
    public Serializer<T> serializer() {
        return this.jsonSerializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this.jsonDeserializer;
    }
}
