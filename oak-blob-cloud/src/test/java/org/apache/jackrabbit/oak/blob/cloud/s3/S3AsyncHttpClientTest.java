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

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import com.sun.net.httpserver.HttpServer;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * Exercises the Netty transport used by {@link Utils} without Docker or cloud credentials.
 */
public class S3AsyncHttpClientTest {

    /**
     * Downloads an S3 object through the real asynchronous HTTP codecs on loopback.
     */
    @Test(timeout = 30000)
    public void downloadsObject() throws Exception {
        byte[] content = "netty-response".getBytes(StandardCharsets.UTF_8);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/bucket/key", exchange -> {
            exchange.sendResponseHeaders(200, content.length);
            try (OutputStream response = exchange.getResponseBody()) {
                response.write(content);
            }
        });
        server.start();
        try (S3AsyncClient client = S3AsyncClient.builder()
                .endpointOverride(URI.create("http://127.0.0.1:" + server.getAddress().getPort()))
                .region(Region.US_EAST_1)
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .forcePathStyle(true)
                .httpClientBuilder(NettyNioAsyncHttpClient.builder()
                        .proxyConfiguration(ProxyConfiguration.builder()
                                .useSystemPropertyValues(false)
                                .useEnvironmentVariableValues(false).build()))
                .build()) {
            ResponseBytes<GetObjectResponse> response = client.getObject(
                    GetObjectRequest.builder().bucket("bucket").key("key").build(),
                    AsyncResponseTransformer.toBytes()).get(10, TimeUnit.SECONDS);
            Assert.assertArrayEquals(content, response.asByteArray());
        } finally {
            server.stop(0);
        }
    }
}
