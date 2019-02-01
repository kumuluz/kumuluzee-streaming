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
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.logging.Logger;

/**
 * Runnable for Kafka consumers.
 *
 * @author Matija Kljun
 * @since 1.0.0
 */
public class ConsumerRunnable implements Runnable {

    private static final long POLL_TIMEOUT_DEFAULT = 2000;
    private static final int MAX_RETRIES_BEFORE_REBALANCE_DEFAULT = 3;

    private KafkaConsumer consumer;
    private final List<String> topics;
    private final Method method;
    private Map<String, Object> consumerConfig;
    private long pollTimeout;
    private int maxRetriesBeforeRebalance;
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
        if (confUtil.getLong("kumuluzee.streaming.kafka.poll-timeout").isPresent())
            pollTimeout = confUtil.getLong("kumuluzee.streaming.kafka.poll-timeout").get();
        else pollTimeout = POLL_TIMEOUT_DEFAULT;
        if (confUtil.getInteger("kumuluzee.streaming.kafka.max-retries-before-rebalance").isPresent())
            maxRetriesBeforeRebalance = confUtil.getInteger("kumuluzee.streaming.kafka.max-retries-before-rebalance").get();
        else maxRetriesBeforeRebalance = MAX_RETRIES_BEFORE_REBALANCE_DEFAULT;
    }

    @Override
    public void run() {

        while (true) {
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

                        Object object = ctor.newInstance(consumer);
                        ConsumerRebalanceListener listener = (ConsumerRebalanceListener) object;

                        consumer.subscribe(topics, listener);
                    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
                        log.severe(e.getMessage());
                    }
                } else
                    consumer.subscribe(topics);

                int retries = 0;
                while (true) {
                    try {

                        ConsumerRecords<?, ?> records = consumer.poll(Duration.of(pollTimeout, ChronoUnit.MILLIS));

                        Iterator<? extends ConsumerRecord<?, ?>> recordsIterator = records.iterator();

                        Map<TopicPartition, Long> nacks = new HashMap<>();

                        if (batchListener) {

                            List<ConsumerRecord> consumerRecordsList = new ArrayList<>();
                            recordsIterator.forEachRemaining(consumerRecordsList::add);

                            try {

                                if (consumerConfig.get("enable.auto.commit") != null &&
                                        consumerConfig.get("enable.auto.commit").equals("false") &&
                                        method.getParameterCount() > 1) {
                                    Acknowledgement ack = new Acknowledgement(this);

                                    method.invoke(instance, consumerRecordsList, ack);

                                    if (!ack.isAcknowledged()) {
                                        // may be partly acknowledged, check if correctly
                                        Map<TopicPartition, Long> maxOffsets = new HashMap<>();
                                        Map<TopicPartition, Long> minOffsets = new HashMap<>();
                                        records.forEach(cr -> {
                                            TopicPartition tp = new TopicPartition(cr.topic(), cr.partition());
                                            maxOffsets.compute(tp, (key, value) ->
                                                    (value == null) ? cr.offset() : Long.max(value, cr.offset()));
                                            minOffsets.compute(tp, (key, value) ->
                                                    (value == null) ? cr.offset() : Long.min(value, cr.offset()));
                                        });

                                        Map<TopicPartition, Long> ackedPartitions = ack.getAcknowledgedPartitions();

                                        for (TopicPartition tp : maxOffsets.keySet()) {
                                            if (!ackedPartitions.containsKey(tp)) {
                                                nacks.put(tp, minOffsets.get(tp));
                                            } else if (ackedPartitions.get(tp) < maxOffsets.get(tp) + 1) {
                                                nacks.put(tp, ackedPartitions.get(tp) - 1);
                                            }
                                        }
                                    }
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
                                        ConsumerRecord<?, ?> record = recordsIterator.next();
                                        TopicPartition tp = new TopicPartition(record.topic(), record.partition());

                                        if (nacks.containsKey(tp)) {
                                            log.warning("Previous message from " + tp + " has not been acknowledged." +
                                                    " Skipping next message in the same partition.");
                                            continue;
                                        }

                                        OffsetAndMetadata oam = new OffsetAndMetadata(record.offset() + 1,
                                                record.leaderEpoch(),
                                                "");
                                        Acknowledgement ack = new Acknowledgement(this, tp, oam);

                                        method.invoke(instance, record, ack);

                                        if (!ack.isAcknowledged()) {
                                            nacks.put(tp, record.offset());
                                        }
                                    } else {
                                        method.invoke(instance, (ConsumerRecord) recordsIterator.next());
                                    }

                                } catch (Exception e) {

                                    log.warning("Error at invoking consumer method: " + e.toString() + e.getCause());

                                }
                            }

                        }

                        for (Map.Entry<TopicPartition, Long> entry : nacks.entrySet()) {
                            consumer.seek(entry.getKey(), entry.getValue());
                        }
                        if (!nacks.isEmpty()) {
                            if (retries >= maxRetriesBeforeRebalance) {
                                log.warning("Max retries reached, trying to rebalance.");
                                break;
                            }

                            retries++;
                            log.warning("Nacks occurred. Waiting before retrying read. Retry attempt: " + retries);
                            Thread.sleep(this.pollTimeout);
                        } else {
                            retries = 0;
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
                break;
            } finally {
                consumer.close();
            }
        }
    }

    public void ack() {
        consumer.commitSync();
    }

    public void ack(Map<TopicPartition, OffsetAndMetadata> offsets) {
        consumer.commitSync(offsets);
    }

    public void shutdown() {
        consumer.wakeup();
    }
}