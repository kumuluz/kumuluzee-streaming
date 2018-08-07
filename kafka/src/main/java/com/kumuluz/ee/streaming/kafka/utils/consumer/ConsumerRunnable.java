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

import com.kumuluz.ee.configuration.utils.ConfigurationUtil;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.WakeupException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * @author Matija Kljun
 */
public class ConsumerRunnable implements Runnable {

    private KafkaConsumer consumer;
    private final List<String> topics;
    private final Method method;
    private Map<String, Object> consumerConfig;
    private long pollTimeout;
    private boolean batchListener;
    private Object instance;
    private Class<?> listenerClass;

    private static final Logger log = Logger.getLogger(ConsumerRunnable.class.getName());

    public ConsumerRunnable(Object instance, Map<String, Object> consumerConfig, List<String> topics, Method method,
                            boolean batchListener, Class<?> listenerClass) {

        this.topics = topics;
        this.method = method;
        this.consumerConfig = consumerConfig;
        this.batchListener = batchListener;
        this.instance = instance;
        this.listenerClass = listenerClass;

        ConfigurationUtil confUtil = ConfigurationUtil.getInstance();
        try {
            if (confUtil.get("kumuluzee.streaming.kafka.poll-timeout").isPresent())
                pollTimeout = Long.parseLong(confUtil.get("kumuluzee.streaming.kafka.poll-timeout").get());
            else pollTimeout = Long.MAX_VALUE;
        } catch (Exception e) {
            log.warning("Incorrect value of poll-timeout in configuration: " + e.toString());
            pollTimeout = Long.MAX_VALUE;
        }
    }

    @Override
    public void run() {

        try {
            this.consumer = new KafkaConsumer<>(consumerConfig);
        } catch (ConfigException e) {
            log.severe("Consumer config exception: " + e.toString());
        } catch (KafkaException e) {
            log.severe("Kafka exception: " + e.toString());
        } catch (Exception e) {
            log.severe("Failed to initialize Consumer: " + e.toString());
        }

        try {

            if (this.listenerClass != null) {
                try {
                    Constructor<?> ctor = this.listenerClass.getConstructor(KafkaConsumer.class);

                    Object object = ctor.newInstance(new Object[]{consumer});
                    ConsumerRebalanceListener listener = (ConsumerRebalanceListener) object;

                    consumer.subscribe(topics, listener);
                } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
                    log.severe(e.getMessage());
                }
            } else
                consumer.subscribe(topics);

            while (true) {
                try {

                    ConsumerRecords records = consumer.poll(pollTimeout);

                    Iterator<ConsumerRecord> recordsIterator = records.iterator();

                    if (batchListener) {

                        List<ConsumerRecord> consumerRecordsList = new ArrayList<>();
                        recordsIterator.forEachRemaining(consumerRecordsList::add);

                        try {

                            if (consumerConfig.get("enable.auto.commit") != null &&
                                    consumerConfig.get("enable.auto.commit").equals("false") &&
                                    method.getParameterCount() > 1) {
                                Acknowledgement ack = new Acknowledgement(this);

                                method.invoke(instance, consumerRecordsList, ack);
                            } else {
                                method.invoke(instance, consumerRecordsList);
                            }

                        } catch (Exception e) {

                            log.warning("Error at invoking consumer method: " + e.toString() + e.getCause());

                        }
                    } else {
                        while (recordsIterator.hasNext()) {
                            try {

                                if (consumerConfig.get("enable.auto.commit") != null &&
                                        consumerConfig.get("enable.auto.commit").equals("false") &&
                                        method.getParameterCount() > 1) {
                                    Acknowledgement ack = new Acknowledgement(this);

                                    method.invoke(instance, (ConsumerRecord) recordsIterator.next(), ack);
                                } else {
                                    method.invoke(instance, (ConsumerRecord) recordsIterator.next());
                                }

                            } catch (Exception e) {

                                log.warning("Error at invoking consumer method: " + e.toString() + e.getCause());

                            }
                        }
                    }


                } catch (Exception e) {
                    log.warning(e.toString());
                    break;
                }
            }
        } catch (InvalidTopicException e) {
            log.warning(e.toString());
        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            consumer.close();
        }
    }

    public void ack() {
        try {
            consumer.commitSync();
        } catch (Exception e) {
            log.warning(e.toString());
        }

    }

    public void ack(Map<TopicPartition, OffsetAndMetadata> offsets) {
        try {
            consumer.commitSync(offsets);
        } catch (Exception e) {
            log.warning(e.toString());
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }
}