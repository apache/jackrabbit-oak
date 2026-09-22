/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v12;

import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import org.junit.Test;
import reactor.core.publisher.Mono;

import java.net.URL;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the HTTP logging pipeline policy. Verbose logging is gated by a system property
 * read at construction time, so the two tests construct separate instances to cover both the
 * pass-through and the logging branch.
 */
public class AzureHttpRequestLoggingPolicyV12Test {

    private static final String VERBOSE_FLAG = "blob.azure.v12.http.verbose.enabled";

    @Test
    public void process_verboseDisabled_passesResponseThroughUnchanged() {
        AzureHttpRequestLoggingPolicyV12 policy = new AzureHttpRequestLoggingPolicyV12();

        HttpResponse response = mock(HttpResponse.class);
        HttpPipelineNextPolicy next = mock(HttpPipelineNextPolicy.class);
        when(next.process()).thenReturn(Mono.just(response));
        HttpPipelineCallContext ctx = mock(HttpPipelineCallContext.class);

        HttpResponse result = policy.process(ctx, next).block();

        assertSame(response, result);
    }

    @Test
    public void process_verboseEnabled_logsRequestDetailsAndPassesThrough() throws Exception {
        System.setProperty(VERBOSE_FLAG, "true");
        try {
            AzureHttpRequestLoggingPolicyV12 policy = new AzureHttpRequestLoggingPolicyV12();

            HttpResponse response = mock(HttpResponse.class);
            when(response.getStatusCode()).thenReturn(200);
            HttpPipelineNextPolicy next = mock(HttpPipelineNextPolicy.class);
            when(next.process()).thenReturn(Mono.just(response));

            HttpRequest request = mock(HttpRequest.class);
            when(request.getHttpMethod()).thenReturn(HttpMethod.GET);
            when(request.getUrl()).thenReturn(new URL("https://acct.blob.core.windows.net/c/k"));
            HttpPipelineCallContext ctx = mock(HttpPipelineCallContext.class);
            when(ctx.getHttpRequest()).thenReturn(request);

            HttpResponse result = policy.process(ctx, next).block();

            assertSame(response, result);
        } finally {
            System.clearProperty(VERBOSE_FLAG);
        }
    }
}
