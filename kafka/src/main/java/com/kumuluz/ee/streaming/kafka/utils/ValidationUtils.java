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
package com.kumuluz.ee.streaming.kafka.utils;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Utils for producer/consumer validation.
 *
 * @author Urban Malc
 * @since 1.2.0
 */
public class ValidationUtils {

    public static Type getSerializerType(Class serializer, boolean forSerializer) {
        String serializerName = (forSerializer) ? "Serializer" : "Deserializer";

        if (serializer == null) {
            throw new IllegalArgumentException(serializerName + " cannot be null");
        }

        Type[] interfaces = serializer.getGenericInterfaces();
        Type serializerType = null;

        Class<?> serializerClass = (forSerializer) ? Serializer.class : Deserializer.class;

        for (Type iface : interfaces) {
            if (iface instanceof ParameterizedType &&
                    ((ParameterizedType) iface).getRawType().equals(serializerClass)) {
                serializerType = ((ParameterizedType) iface).getActualTypeArguments()[0];
            }
        }

        if (serializerType == null) {
            throw new IllegalArgumentException(serializerName + " does not implement the " + serializerClass.getName() +
                    " interface.");
        }

        return serializerType;
    }
}
