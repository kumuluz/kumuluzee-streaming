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
package com.kumuluz.ee.streaming.kafka.tests;

import com.kumuluz.ee.streaming.kafka.tests.beans.InvalidValueTypeConsumer;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.ShouldThrowException;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.testng.annotations.Test;

import javax.enterprise.inject.spi.DeploymentException;

/**
 * Invalid deployment test.
 *
 * @author Urban Malc
 * @since 1.2.0
 */
@Test
public class InvalidValueTypeConsumerTest extends Arquillian {

    @Deployment
    @ShouldThrowException(value = DeploymentException.class, testable = true)
    public static JavaArchive createDeployment() {
        return ShrinkWrap.create(JavaArchive.class)
                .addClass(InvalidValueTypeConsumer.class)
                .addAsResource("config.yml")
                .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");
    }

    public void invalidConfigurationTest() {
        // see @ShouldThrowException above
    }
}
