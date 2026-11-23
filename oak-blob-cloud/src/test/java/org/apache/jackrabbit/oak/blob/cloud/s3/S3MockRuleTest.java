/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.blob.cloud.s3;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.testcontainers.containers.GenericContainer;

/**
 * Tests for {@link S3MockRule}.
 */
public class S3MockRuleTest {

    @Test
    public void httpEndpointUsesRemoteDockerHostname() throws Throwable {
        assertHttpEndpoint("docker.example.com", "http://docker.example.com:49152");
    }

    @Test
    public void httpEndpointUsesRemoteDockerAddress() throws Throwable {
        assertHttpEndpoint("192.0.2.10", "http://192.0.2.10:49152");
    }

    @Test
    public void httpEndpointRetainsLoopbackHost() throws Throwable {
        assertHttpEndpoint("127.0.0.1", "http://127.0.0.1:49152");
    }

    private static void assertHttpEndpoint(String host, String expectedEndpoint) throws Throwable {
        try (MockedConstruction<?> containers = Mockito.mockConstruction(GenericContainer.class,
                Mockito.withSettings().defaultAnswer(Mockito.RETURNS_SELF), (container, context) -> {
                    Mockito.when(container.getHost()).thenReturn(host);
                    Mockito.when(container.getMappedPort(S3MockRule.HTTP_PORT)).thenReturn(49152);
                })) {
            S3MockRule rule = new S3MockRule();
            rule.before();
            try {
                Assert.assertEquals(expectedEndpoint, rule.getHttpEndpoint());
                Assert.assertEquals(49152, rule.getHttpPort());
            } finally {
                rule.after();
            }
        }
    }
}
