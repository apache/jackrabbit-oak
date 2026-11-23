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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import java.net.InetSocketAddress;

import com.azure.core.http.ProxyOptions;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.util.Configuration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link NettyAsyncHttpClientBuilder} with the dependencies embedded by the Azure data store.
 */
public class AzureHttpClientTest {

    /**
     * Checks client construction without a proxy as a control for OAK-12468.
     */
    @Test
    public void buildsWithoutProxy() {
        Assert.assertNotNull(new NettyAsyncHttpClientBuilder()
                .configuration(Configuration.NONE)
                .build());
    }

    /**
     * Exercises the Reactor Netty proxy builder that failed with NoSuchMethodError in OAK-12468.
     */
    @Test
    public void buildsWithProxy() {
        ProxyOptions proxy = new ProxyOptions(ProxyOptions.Type.HTTP,
                InetSocketAddress.createUnresolved("proxy.example", 3128));

        Assert.assertNotNull(new NettyAsyncHttpClientBuilder()
                .configuration(Configuration.NONE)
                .proxy(proxy)
                .build());
    }

    /**
     * Exercises proxy discovery without changing the process environment or global configuration.
     */
    @Test
    public void buildsWithProxyFromConfiguration() {
        Configuration configuration = Configuration.getGlobalConfiguration().clone()
                .put("java.net.useSystemProxies", "true")
                .put(Configuration.PROPERTY_HTTPS_PROXY, "http://proxy.example:3128");

        Assert.assertNotNull(ProxyOptions.fromConfiguration(configuration, true));
        Assert.assertNotNull(new NettyAsyncHttpClientBuilder()
                .configuration(configuration)
                .build());
    }

    /**
     * Checks that bypass rules do not hide proxy construction failures before a request is sent.
     */
    @Test
    public void buildsWithProxyAndNonProxyHosts() {
        ProxyOptions proxy = new ProxyOptions(ProxyOptions.Type.HTTP,
                InetSocketAddress.createUnresolved("proxy.example", 3128))
                .setNonProxyHosts(".*\\.windows\\.net|login\\.microsoftonline\\.com");

        Assert.assertNotNull(new NettyAsyncHttpClientBuilder()
                .configuration(Configuration.NONE)
                .proxy(proxy)
                .build());
    }

    /**
     * Checks the authenticated HTTP proxy path, which uses the Azure SDK's own proxy handler.
     */
    @Test
    public void buildsWithAuthenticatedProxy() {
        ProxyOptions proxy = new ProxyOptions(ProxyOptions.Type.HTTP,
                InetSocketAddress.createUnresolved("proxy.example", 3128))
                .setCredentials("test-user", "test-password");

        Assert.assertNotNull(new NettyAsyncHttpClientBuilder()
                .configuration(Configuration.NONE)
                .proxy(proxy)
                .build());
    }
}
