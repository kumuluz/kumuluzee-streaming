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

import com.kumuluz.ee.streaming.common.annotations.StreamProcessor;
import com.kumuluz.ee.streaming.common.annotations.StreamProcessorController;
import com.kumuluz.ee.streaming.common.utils.AnnotatedInstance;
import com.kumuluz.ee.streaming.common.utils.StreamsInitializerExtension;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.logging.Logger;

/**
 * @author Matija Kljun
 */
public class KafkaStreamsInitializerExtension implements StreamsInitializerExtension {

    private static final Logger log = Logger.getLogger(KafkaStreamsInitializerExtension.class.getName());

    private KafkaStreamsFactory kafkaStreamsFactory;

    private List<AnnotatedInstance<StreamProcessor>> instanceList = new ArrayList<>();
    private Map<String, StreamsController> streamBeans = new HashMap<>();
    private final Map<Field, StreamsController> fieldValues = new HashMap<>();

    public <X> void processAnnotatedTypes(@Observes ProcessAnnotatedType<X> pat) {

        AnnotatedType<X> at = pat.getAnnotatedType();

        for (AnnotatedMethod method : at.getMethods()) {
            if (method.getAnnotation(StreamProcessor.class) != null) {

                StreamProcessor annotation = method.getAnnotation(StreamProcessor.class);

                instanceList.add(new AnnotatedInstance<>( at.getJavaClass(), method.getJavaMember(), annotation));
            }
        }
    }

    public <X> void afterTypeDiscovery(@Observes AfterTypeDiscovery atd, BeanManager bm) {

        if (instanceList.size() > 0) {

            kafkaStreamsFactory = new KafkaStreamsFactory();

            for (AnnotatedInstance<StreamProcessor> inst : instanceList) {

                log.finer("FOUND : " + inst.getMethod().getName());

                StreamProcessor annotation = inst.getAnnotation();

                String id = annotation.id();

                StreamsController sb = kafkaStreamsFactory.createStreamProcessor();//kafkaStreamsFactory.createStreamProcessor(instance, configName, method, id, autoStart);

                if(streamBeans.containsKey(id)) {
                    log.warning("StreamProducer id must be unique!");
                } else {
                    log.info("init StreamsController");
                    streamBeans.put(id, sb);

                }
            }

        }
    }

    @Override
    public <T> void processInjectionTarget(final @Observes ProcessInjectionTarget<T> pit) {

        AnnotatedType<T> at = pit.getAnnotatedType();

        boolean isAnnotated = false;

        for (AnnotatedField field : at.getFields()) {
            if (field.getAnnotation(StreamProcessorController.class) != null) {

                StreamProcessorController annotation = field.getAnnotation(StreamProcessorController.class);

                if (streamBeans.containsKey(annotation.id())) {
                    StreamsController sb = streamBeans.get(annotation.id());
                    isAnnotated = true;
                    Field fieldMember = field.getJavaMember();
                    fieldValues.put(fieldMember, sb);
                } else {
                    log.warning("There is no StreamsController instance with id { " + annotation.id() + " }");
                }
            }
        }

        if (isAnnotated) {

            final InjectionTarget<T> it = pit.getInjectionTarget();
            InjectionTarget<T> wrapped = new InjectionTarget<T>() {
                @Override
                public void inject(T instance, CreationalContext<T> ctx) {
                    it.inject(instance, ctx);
                    for (Map.Entry<Field, StreamsController> streamBean : fieldValues.entrySet()) {
                        try {
                            Field key = streamBean.getKey();
                            key.setAccessible(true);
                            Class<?> baseType = key.getType();
                            StreamsController value = streamBean.getValue();
                            log.info(instance.getClass().toString());
                            log.info(key.getDeclaringClass().toString());
                            if (baseType == StreamsController.class) {
                                if(instance.getClass().toString().equals(key.getDeclaringClass().toString()))
                                    key.set(instance, value);
                            } else {
                                log.severe("Type "+ baseType + " not supported, @StreamProcessorController should annotate field of type " + StreamsController.class + ".");
                            }
                        } catch (Exception e) {
                            log.severe("Error when injecting @StreamProcessorController annotated field. " + e.toString());
                        }
                    }
                }


                @Override
                public void postConstruct(T instance) {
                    it.postConstruct(instance);
                }


                @Override
                public void preDestroy(T instance) {
                    it.dispose(instance);
                }


                @Override
                public void dispose(T instance) {
                    it.dispose(instance);
                }


                @Override
                public Set<InjectionPoint> getInjectionPoints() {
                    return it.getInjectionPoints();
                }


                @Override
                public T produce(CreationalContext<T> ctx) {
                    return it.produce(ctx);
                }
            };

            pit.setInjectionTarget(wrapped);

        }
    }

    public <X> void after(@Observes AfterDeploymentValidation adv, BeanManager bm) {
        if (instanceList.size() > 0) {

            kafkaStreamsFactory = new KafkaStreamsFactory();

            for (AnnotatedInstance<StreamProcessor> inst : instanceList) {

                Method method = inst.getMethod();
                StreamProcessor annotation = inst.getAnnotation();

                String configName = annotation.config();
                String id = annotation.id();
                boolean autoStart = annotation.autoStart();

                if(streamBeans.containsKey(id)) {

                    StreamsController sb = streamBeans.get(id);

                    Object instance = CDI.current().select(inst.getClazz()).get();

                    kafkaStreamsFactory.setStreamProcessor(sb, instance, configName, method, id, autoStart);

                    log.info("Kafka Streams start");
                }

            }

        }
    }

}
