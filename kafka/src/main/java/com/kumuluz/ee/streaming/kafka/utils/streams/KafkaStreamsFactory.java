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

import com.kumuluz.ee.streaming.common.config.ConfigLoader;
import com.kumuluz.ee.streaming.common.utils.StreamProcessorFactory;
import com.kumuluz.ee.streaming.kafka.config.KafkaStreamsConfigLoader;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

import java.lang.reflect.Method;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Factory for Kafka stream processors.
 *
 * @author Matija Kljun
 * @since 1.0.0
 */
public class KafkaStreamsFactory implements StreamProcessorFactory<StreamsController> {

    private static final Logger log = Logger.getLogger(KafkaStreamsFactory.class.getName());

    @Override
    public StreamsController createStreamProcessor() {
        return new StreamsController();
    }

    public void setStreamProcessor(StreamsController sb,
                                                Object instance,
                                                String configName,
                                                Method method,
                                                String id,
                                                boolean autoStart) {


        Properties streamsConfig = ConfigLoader.asProperties(KafkaStreamsConfigLoader.getConfig(configName));

        if (method.getParameterCount() > 0) {
            log.severe("StreamProcessor annotated method { " + method.getName() + " } shouldn't have any parameters!");
            return;
        }

        Class methodReturnType = method.getReturnType();

        if (!methodReturnType.equals(Topology.class) && !methodReturnType.equals(StreamsBuilder.class)) {
            log.severe("StreamProcessor annotated method { " + method.getName() + " } must return a Topology or StreamBuilder instance!");
            return;
        }

        Object builderObject = null;

        try {
            builderObject = method.invoke(instance);
        } catch (Exception e) {
            log.warning("Error at invoking StreamProcessor annotated method { " + method.getName() + " } " + e.toString() + " " + e.getCause());
        }

        if (builderObject == null)
            log.warning("Error at invoking StreamProcessor annotated method { " + method.getName() + " } ");

        if (builderObject instanceof StreamsBuilder) {
            sb.setStreams(((StreamsBuilder) builderObject).build(), streamsConfig);
        } else if (builderObject instanceof Topology) {
            sb.setStreams((Topology) builderObject, streamsConfig);
        }

        if(autoStart) {
            try {
                sb.start();
            } catch (Exception e) {
                log.severe(e.toString());
            }
        }
    }

}
