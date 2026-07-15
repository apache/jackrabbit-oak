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
package org.apache.jackrabbit.oak.segment.azure;

import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class MockAzureHttpResponse extends HttpResponse {

    private final int statusCode;
    private final String body;
    private HttpHeaders headers;

    public MockAzureHttpResponse(int statusCode, String body) {
        super(null);
        this.statusCode = statusCode;
        this.body = body;
    }

    @Override
    public int getStatusCode() {
        return statusCode;
    }

    @Override
    public String getHeaderValue(String name) {
        return null; // Simplified for this example
    }

    @Override
    public HttpHeaders getHeaders() {
        return this.headers;
    }

    public void setHeaders(HttpHeaders headers) {
        this.headers = headers;
    }


    @Override
    public Flux<ByteBuffer> getBody() {
        return null;
    }

    @Override
    public Mono<byte[]> getBodyAsByteArray() {
        return Mono.just(body.getBytes());
    }

    @Override
    public Mono<String> getBodyAsString() {
        return null;
    }

    @Override
    public Mono<String> getBodyAsString(Charset charset) {
        return null;
    }
}

