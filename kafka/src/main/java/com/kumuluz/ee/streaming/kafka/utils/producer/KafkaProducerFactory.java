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

package com.kumuluz.ee.streaming.kafka.utils.producer;

import com.kumuluz.ee.streaming.common.annotations.StreamProducer;
import com.kumuluz.ee.streaming.common.utils.ProducerFactory;
import com.kumuluz.ee.streaming.kafka.config.KafkaProducerConfigLoader;
import com.kumuluz.ee.streaming.kafka.utils.KafkaExtension;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.config.ConfigException;

import javax.enterprise.inject.spi.Annotated;
import javax.enterprise.inject.spi.InjectionPoint;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Factory for Kafka producers.
 *
 * @author Matija Kljun
 * @since 1.0.0
 */
public class KafkaProducerFactory implements ProducerFactory<Producer> {

    private static final Logger LOG = Logger.getLogger(KafkaProducerFactory.class.getName());

    private static KafkaProducerFactory instance;

    private Map<String, Producer> producers = new HashMap<>();
    private Map<Annotated, Producer> producersWithConfigOverrides = new HashMap<>();

    private static synchronized void init() {
        if (instance == null) {
            instance = new KafkaProducerFactory();
        }
    }

    public static KafkaProducerFactory getInstance() {
        if (instance == null) {
            init();
        }

        return instance;
    }

    private KafkaProducerFactory() {
    }

    @Override
    public Producer createProducer(Map<String, Object> producerConfig) {

        Producer producer = null;

        try {
            producer = new KafkaProducer(producerConfig);
            LOG.info("Created Kafka Producer.");
        } catch (ConfigException e) {
            LOG.severe("Producer config exception: " + e.toString());
        }

        return producer;
    }

    public Producer getProducer(InjectionPoint injectionPoint) {

        if (!KafkaExtension.isExtensionEnabled()) {
            return null;
        }

        StreamProducer annotation = injectionPoint.getAnnotated().getAnnotation(StreamProducer.class);
        String config = annotation.config();

        if (annotation.configOverrides().length == 0) {
            // cache by config name if no overrides present
            if (!producers.containsKey(config)) {
                Producer producer = createProducer(KafkaProducerConfigLoader.getConfig(config,
                        annotation.configOverrides()));
                producers.put(config, producer);
            }

            return producersWithConfigOverrides.get(injectionPoint.getAnnotated());
        } else {
            // cache by injection point if overrides present
            if (!producersWithConfigOverrides.containsKey(injectionPoint.getAnnotated())) {
                Producer producer = createProducer(KafkaProducerConfigLoader.getConfig(config,
                        annotation.configOverrides()));
                producersWithConfigOverrides.put(injectionPoint.getAnnotated(), producer);
            }

            return producersWithConfigOverrides.get(injectionPoint.getAnnotated());
        }
    }
}
