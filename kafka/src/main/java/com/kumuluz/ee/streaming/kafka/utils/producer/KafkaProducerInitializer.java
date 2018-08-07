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
import com.kumuluz.ee.streaming.kafka.config.KafkaProducerConfigLoader;
import org.apache.kafka.clients.producer.Producer;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

/**
 * Producer for KafkaProducer annotation.
 *
 * @author Matija Kljun
 */
@ApplicationScoped
public class KafkaProducerInitializer {

    private Map<String, Producer> producers = new HashMap<>();

    @Inject
    private KafkaProducerFactory kafkaProducerFactory;

    @Produces
    @StreamProducer
    public Producer getProducer(InjectionPoint injectionPoint) {

        String config = injectionPoint.getAnnotated().getAnnotation(StreamProducer.class).config();

        if (producers.containsKey(config)) {

            Producer producer = producers.get(config);

            if (producer != null)
                return producer;

        } else {
            Producer producer = kafkaProducerFactory.createProducer(KafkaProducerConfigLoader.getConfig(config));
            producers.put(config, producer);

            if (producer != null)
                return producer;

        }

        return null;

    }
}
