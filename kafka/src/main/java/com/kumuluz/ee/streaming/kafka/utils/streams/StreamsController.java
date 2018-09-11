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

package com.kumuluz.ee.streaming.kafka.utils.streams;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.StreamsMetadata;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Kafka streams controller.
 *
 * @author Matija Kljun
 * @since 1.0.0
 */
public class StreamsController {

    private KafkaStreams streams;

    public void setStreams(Topology builder, Properties properties) {
        this.streams = new KafkaStreams(builder, properties);
    }

    public void start() {
        this.streams.start();
    }

    public void close() {
        this.streams.close();
    }

    public boolean close(long timeout, TimeUnit timeUnit) {
        return this.streams.close(timeout, timeUnit);
    }

    public void setStateListener(KafkaStreams.StateListener listener) {
        this.streams.setStateListener(listener);
    }

    public synchronized KafkaStreams.State state() {
        return this.streams.state();
    }

    public Map<MetricName, ? extends Metric> metrics() {
        return this.streams.metrics();
    }

    public String toString() {
        return this.streams.toString();
    }

    public void cleanUp() {
        this.streams.cleanUp();
    }

    public void setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler eh) {
        this.streams.setUncaughtExceptionHandler(eh);
    }

    public Collection<StreamsMetadata> allMetadata() {
        return this.streams.allMetadata();
    }

    public Collection<StreamsMetadata> allMetadataForStore(String storeName) {
        return this.streams.allMetadataForStore(storeName);
    }

    public <K> StreamsMetadata metadataForKey(String storeName, K key, Serializer<K> keySerializer) {
        return this.streams.metadataForKey(storeName, key, keySerializer);
    }

    public <K> StreamsMetadata metadataForKey(String storeName, K key,  StreamPartitioner<? super K,?> partitioner) {
        return this.streams.metadataForKey(storeName, key, partitioner);
    }

    public <T> T store(String storeName, QueryableStoreType<T> queryableStoreType) {
        return this.streams.store(storeName, queryableStoreType);
    }
}
