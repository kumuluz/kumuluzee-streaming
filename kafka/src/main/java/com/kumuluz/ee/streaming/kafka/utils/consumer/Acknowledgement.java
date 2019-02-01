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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Kafka message acknowledgement.
 *
 * @author Matija Kljun
 * @since 1.0.0
 */
public class Acknowledgement {

    private ConsumerRunnable consumer;
    private TopicPartition tp;
    private OffsetAndMetadata oam;
    private boolean acknowledged;
    private boolean batch;

    private Map<TopicPartition, Long> acknowledgedPartitions;

    public Acknowledgement(ConsumerRunnable consumer) {
        this(consumer, null, null);
    }

    public Acknowledgement(ConsumerRunnable consumer, TopicPartition tp, OffsetAndMetadata oam) {
        this.consumer = consumer;
        this.tp = tp;
        this.oam = oam;
        this.acknowledged = false;
        this.batch = tp == null && oam == null;

        if (batch) {
            this.acknowledgedPartitions = new HashMap<>();
        }
    }

    public void acknowledge() {
        if (!batch) {
            consumer.ack(Collections.singletonMap(tp, oam));
        } else {
            consumer.ack();
        }
        this.acknowledged = true;
    }

    public void acknowledge(java.util.Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (!batch) {
            throw new IllegalStateException("This method can only be used when using batch message consuming");
        }
        consumer.ack(offsets);
        offsets.forEach((tp, oam) ->
                acknowledgedPartitions.compute(tp, (k, v) ->
                        (v == null) ? oam.offset() : Long.max(oam.offset(), v)));
    }

    public boolean isAcknowledged() {
        return acknowledged;
    }

    public Map<TopicPartition, Long> getAcknowledgedPartitions() {
        return acknowledgedPartitions;
    }
}
