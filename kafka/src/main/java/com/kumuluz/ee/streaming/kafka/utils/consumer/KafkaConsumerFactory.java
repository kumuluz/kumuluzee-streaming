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

import com.kumuluz.ee.streaming.common.annotations.ConfigurationOverride;
import com.kumuluz.ee.streaming.common.utils.ConsumerFactory;
import com.kumuluz.ee.streaming.kafka.config.KafkaConsumerConfigLoader;
import com.kumuluz.ee.streaming.kafka.utils.ValidationUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.enterprise.inject.spi.AfterDeploymentValidation;
import javax.enterprise.inject.spi.DeploymentException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of consumer factory for Kafka.
 *
 * @author Matija Kljun
 * @since 1.0.0
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
                                           Class<?> listenerClass,
                                           ConfigurationOverride[] overrides, AfterDeploymentValidation adv) {

        if (topics.length == 0) {
            topics = new String[1];
            topics[0] = method.getName();
        }

        Map<String, Object> consumerConfig = KafkaConsumerConfigLoader.getConfig(configName, overrides);

        if(!groupId.equals("")) {
            consumerConfig.put("group.id", groupId);
        }

        boolean configurationCorrect = true;
        if (batchListener) {

            if (!method.getParameterTypes()[0].isAssignableFrom(List.class)) {
                log.severe("Incorrect StreamListener method parameters, if batchListener is set to true the " +
                        "method expects a List of ConsumerRecord objects.");
                configurationCorrect = false;
            } else {
                if (method.getGenericParameterTypes()[0] instanceof ParameterizedType) {
                    configurationCorrect = validateConsumerRecord(((ParameterizedType)method
                            .getGenericParameterTypes()[0]).getActualTypeArguments()[0], consumerConfig);
                } else {
                    log.warning("StreamListener List parameter is not generic. Type safety cannot be ensured.");
                }
            }
        } else {
            // non-batch listener
            if (!method.getParameterTypes()[0].isAssignableFrom(ConsumerRecord.class)) {
                log.severe("Incorrect StreamListener method parameters, if batchListener is set to false " +
                        "(default) the method expects a ConsumerRecord parameter.");
                configurationCorrect = false;
            } else {
                if (method.getGenericParameterTypes()[0] instanceof ParameterizedType) {
                    configurationCorrect = validateConsumerRecord(method.getGenericParameterTypes()[0], consumerConfig);
                } else {
                    log.warning("StreamListener List parameter is not generic. Type safety cannot be ensured.");
                }
            }

        }
        if (consumerConfig.get("enable.auto.commit") != null &&
                consumerConfig.get("enable.auto.commit").equals("true") &&
                method.getParameterCount() > 1) {

            log.severe("Incorrect StreamListener method parameters, if the enable-auto-commit is set to true," +
                    " there is no need for Acknowledgement parameter.");
            configurationCorrect = false;

        }

        if (configurationCorrect) {
            return new ConsumerRunnable(instance, consumerConfig, Arrays.asList(topics), method, batchListener,
                    listenerClass);
        } else {
            adv.addDeploymentProblem(new DeploymentException("Configuration for StreamListener " + method.getDeclaringClass().getName() +
                    "#" + method.getName() + " is incorrect."));
            return null;
        }
    }

    private boolean validateConsumerRecord(Type consumerRecord, Map<String, Object> consumerConfig) {
        if (consumerRecord instanceof ParameterizedType) {

            if (!((Class<?>)((ParameterizedType) consumerRecord).getRawType()).isAssignableFrom(ConsumerRecord.class)) {
                log.severe("Parameter class is not ConsumerRecord.");
                return false;
            }

            Type keyType = ((ParameterizedType) consumerRecord).getActualTypeArguments()[0];
            Type valueType = ((ParameterizedType) consumerRecord).getActualTypeArguments()[1];

            // get serializer classes from config
            String keyTypeConfig = (String) consumerConfig.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
            String valueTypeConfig = (String) consumerConfig.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);

            if (keyTypeConfig == null) {
                log.severe("Key deserializer is not specified in the consumer configuration.");
                return false;
            }
            if (valueTypeConfig == null) {
                log.severe("Value deserializer is not specified in the consumer configuration.");
                return false;
            }

            try {
                Class keySerializer = Class.forName(keyTypeConfig);
                Type t = ValidationUtils.getSerializerType(keySerializer, false);

                if (!(t instanceof TypeVariable) && !((Class<?>)keyType).isAssignableFrom((Class<?>) t)) {
                    log.severe("Key serializer type does not match the StreamListener parameter type.");
                    return false;
                }
            } catch (ClassNotFoundException e) {
                log.log(Level.SEVERE, "Key serializer class cannot be found.", e);
            }
            try {
                Class valueSerializer = Class.forName(valueTypeConfig);
                Type t = ValidationUtils.getSerializerType(valueSerializer, false);

                //Some Confluent serializers use Object instead of generic T
                if (!(t instanceof TypeVariable) && !((Class<?>)valueType).isAssignableFrom((Class<?>) t)
                    && !Object.class.equals(t)) {
                    log.severe("Value serializer type does not match the StreamListener parameter type.");
                    return false;
                }
            } catch (ClassNotFoundException e) {
                log.log(Level.SEVERE, "Value serializer class cannot be found.", e);
            }

            return true;
        } else if (!((Class<?>)consumerRecord).isAssignableFrom(ConsumerRecord.class)) {
            log.severe("Parameter class is not ConsumerRecord.");
            return false;
        } else {
            log.warning("StreamListener ConsumerRecord is not generic. Type safety cannot be ensured.");
            return true;
        }
    }
}
