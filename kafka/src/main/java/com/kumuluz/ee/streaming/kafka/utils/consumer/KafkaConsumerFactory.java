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

package com.kumuluz.ee.streaming.kafka.utils.consumer;

import com.kumuluz.ee.streaming.common.utils.ConsumerFactory;
import com.kumuluz.ee.streaming.kafka.config.KafkaConsumerConfigLoader;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * @author Matija Kljun
 */
public class KafkaConsumerFactory implements ConsumerFactory<ConsumerRunnable> {

    private static final Logger log = Logger.getLogger(KafkaConsumerFactory.class.getName());

    @Override
    public ConsumerRunnable createConsumer(Object instance,
                                           String configName,
                                           String groupId,
                                           String[] topics,
                                           Method method,
                                           boolean batchListener,
                                           Class<?> listenerClass) {

        if (topics.length == 0) {
            topics = new String[1];
            topics[0] = method.getName();
        }

        Map<String, Object> consumerConfig = KafkaConsumerConfigLoader.getConfig(configName);

        if(!groupId.equals("")) {
            consumerConfig.put("group.id", groupId);
        }

        if (batchListener && !method.getParameterTypes()[0].isAssignableFrom(List.class)) {

            log.warning("Incorrect StreamListener method parameters, if batchListener is set to true the " +
                    "method expects a List of CosnumerRecord objects.");

        } else if (!batchListener && !method.getParameterTypes()[0].isAssignableFrom(ConsumerRecord.class)) {

            log.warning("Incorrect StreamListener method parameters, if batchListener is set to false " +
                    "(default) the method expects a CosnumerRecord parameter.");

        } else if (consumerConfig.get("enable.auto.commit") != null && consumerConfig.get("enable.auto.commit").equals("true") && method.getParameterCount() > 1) {

            log.warning("Incorrect StreamListener method parameters, if the enable-auto-commit is set to true," +
                    " there is no need for Acknowledgement parameter.");

        } else {

            return new ConsumerRunnable(instance, consumerConfig, Arrays.asList(topics), method, batchListener, listenerClass);
        }

        return null;
    }
}
