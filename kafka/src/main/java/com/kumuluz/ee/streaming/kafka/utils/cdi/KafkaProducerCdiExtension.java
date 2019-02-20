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
package com.kumuluz.ee.streaming.kafka.utils.cdi;

import com.kumuluz.ee.streaming.common.annotations.StreamProducer;
import com.kumuluz.ee.streaming.kafka.config.KafkaProducerConfigLoader;
import com.kumuluz.ee.streaming.kafka.utils.ValidationUtils;
import com.kumuluz.ee.streaming.kafka.utils.producer.KafkaProducerInitializer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.*;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Creates dynamic producers for type-safe {@link Producer} injection.
 *
 * @author Urban Malc
 * @since 1.2.0
 */
public class KafkaProducerCdiExtension implements Extension {

    private static final Logger LOG = Logger.getLogger(KafkaProducerCdiExtension.class.getName());

    private Set<InjectionPoint> injectionPoints = new HashSet<>();

    /**
     * Collects injection points which require a dynamic producer.
     *
     * @param pip injection point
     */
    public void collectProducerInjectionPoints(@Observes ProcessInjectionPoint<?, ?> pip) {
        StreamProducer streamProducer = pip.getInjectionPoint().getAnnotated().getAnnotation(StreamProducer.class);
        // If stream producer is not generic, we don't need dynamic producer, because static one exists.
        if (streamProducer != null && pip.getInjectionPoint().getType() instanceof ParameterizedType) {
            injectionPoints.add(pip.getInjectionPoint());
        }
    }

    /**
     * Validates injection points annotated with {@link StreamProducer}.
     *
     * @param pip injection point
     */
    public void validateInjectionPoint(@Observes ProcessInjectionPoint<?, ?> pip) {

        StreamProducer streamProducer = pip.getInjectionPoint().getAnnotated().getAnnotation(StreamProducer.class);
        if (streamProducer != null) {
            if (!isProducer(pip.getInjectionPoint().getType())) {
                pip.addDefinitionError(new DeploymentException("Injection point '" + pip.getInjectionPoint() +
                        "' annotated with @StreamProducer is not of type " + Producer.class.getName()));
                return;
            }

            if (isParameterizedProducer(pip.getInjectionPoint().getType())) {
                // check if serializer type matches injection point type
                Class<?> injectionKeyType = (Class<?>) ((ParameterizedType) (pip.getInjectionPoint().getType()))
                        .getActualTypeArguments()[0];
                Class<?> injectionValueType = (Class<?>) ((ParameterizedType) (pip.getInjectionPoint().getType()))
                        .getActualTypeArguments()[1];

                Map<String, Object> config = KafkaProducerConfigLoader.getConfig(streamProducer.config(),
                        streamProducer.configOverrides());

                // get serializer classes from config
                String keyType = (String) config.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
                String valueType = (String) config.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);

                if (keyType == null) {
                    pip.addDefinitionError(new DeploymentException("Key serializer for injection point '" + pip.getInjectionPoint() +
                            "' is not specified."));
                }
                if (valueType == null) {
                    pip.addDefinitionError(new DeploymentException("Value serializer for injection point '" + pip.getInjectionPoint() +
                            "' is not specified."));
                }

                try {
                    // String to Class
                    Class keySerializer = Class.forName(keyType);

                    // get type of the serializer
                    Type t = ValidationUtils.getSerializerType(keySerializer, true);

                    // if not generic type and injection type not assignable from serializer type -> error
                    if (!(t instanceof TypeVariable) && !injectionKeyType.isAssignableFrom((Class<?>) t)) {
                        pip.addDefinitionError(new DeploymentException("Key serializer for injection point '" + pip.getInjectionPoint() +
                                "' type does not match the injection point type."));
                    }
                } catch (ClassNotFoundException e) {
                    pip.addDefinitionError(new DeploymentException("Key serializer for injection point '" + pip.getInjectionPoint() +
                            "' cannot be found", e));
                }

                // same for value
                try {
                    Class valueSerializer = Class.forName(valueType);

                    Type t = ValidationUtils.getSerializerType(valueSerializer, true);

                    if (!(t instanceof TypeVariable) && !injectionValueType.isAssignableFrom((Class<?>) t)) {
                        pip.addDefinitionError(new DeploymentException("Value serializer for injection point '" + pip.getInjectionPoint() +
                                "' type does not match the injection point type."));
                    }
                } catch (ClassNotFoundException e) {
                    pip.addDefinitionError(new DeploymentException("Value serializer for injection point '" + pip.getInjectionPoint() +
                            "' cannot be found", e));
                }
            } else {
                LOG.warning("Injection point '" + pip.getInjectionPoint() + "' is not parameterized. Type safety cannot be ensured.");
            }
        }
    }

    private boolean isParameterizedProducer(Type t) {
        return (t instanceof ParameterizedType && ((ParameterizedType) t).getRawType().equals(Producer.class));
    }

    private boolean isProducer(Type t) {
        return isParameterizedProducer(t) || t.equals(Producer.class);
    }

    /**
     * Adds dynamic producers for all injection points collected in the previous step
     * ({@link KafkaProducerCdiExtension#collectProducerInjectionPoints(ProcessInjectionPoint)}). At this step, all
     * injection points are already validated.
     *
     * @param event {@link AfterBeanDiscovery} event
     * @param bm    bean manager
     */
    public void addDynamicProducers(@Observes AfterBeanDiscovery event, BeanManager bm) {

        // create annotated type from producer method
        AnnotatedType<KafkaProducerInitializer> annotatedType = bm.createAnnotatedType(KafkaProducerInitializer.class);
        BeanAttributes<?> beanAttributes = null;
        AnnotatedMethod<? super KafkaProducerInitializer> annotatedMethod = null;
        for (AnnotatedMethod<? super KafkaProducerInitializer> m : annotatedType.getMethods()) {
            if (m.getJavaMember().getName().equals(KafkaProducerInitializer.PRODUCER_METHOD_NAME)) {
                beanAttributes = bm.createBeanAttributes(m);
                annotatedMethod = m;
                break;
            }
        }

        if (beanAttributes != null) {

            HashSet<Type> types = new HashSet<>();
            for (InjectionPoint ip : injectionPoints) {
                types.add(ip.getType());
            }

            // create producer beans for all types, required by the injection points
            for (final Type producerType : types) {

                Bean<?> bean = bm.createBean(new TypesBeanAttributes<Object>(beanAttributes) {

                    @Override
                    public Set<Type> getTypes() {
                        HashSet<Type> result = new HashSet<>();
                        result.add(producerType);
                        return result;
                    }
                }, KafkaProducerInitializer.class, bm.getProducerFactory(annotatedMethod, null));
                event.addBean(bean);
            }
        }
    }
}
