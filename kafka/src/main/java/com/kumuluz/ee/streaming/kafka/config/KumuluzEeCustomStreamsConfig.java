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
package com.kumuluz.ee.streaming.kafka.config;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Custom configuration keys for Kafka streams.
 *
 * @author Urban Malc
 * @since 1.2.0
 */
public class KumuluzEeCustomStreamsConfig {

    public static final String DEFAULT_KEY_SERDE_TYPE = "default.key.serde.type";
    public static final String DEFAULT_VALUE_SERDE_TYPE = "default.value.serde.type";
    public static final String DEFAULT_SERDE_TYPE = "default.serde.type";

    public static List<String> getCustomConfigs() {
        return Stream.of(DEFAULT_KEY_SERDE_TYPE, DEFAULT_VALUE_SERDE_TYPE, DEFAULT_SERDE_TYPE)
                .collect(Collectors.toList());
    }
}
