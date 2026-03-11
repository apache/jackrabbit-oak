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

import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.HttpPipelinePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * An Azure HTTP pipeline policy that falls back to a secondary (DR) blob endpoint
 * when a GET request returns 404 from the primary endpoint.
 * <p>
 * This enables disaster recovery scenarios where the primary storage account is empty
 * or partially synced, and reads should fall back to an offsite backup storage account.
 * Only GET requests are retried on the secondary; write operations are never redirected.
 */
public class ReadFallbackPolicy implements HttpPipelinePolicy {

    private static final Logger log = LoggerFactory.getLogger(ReadFallbackPolicy.class);

    private final String secondaryBlobEndpoint;

    /**
     * @param secondaryBlobEndpoint the full blob endpoint URL of the secondary (backup) storage account,
     *                              e.g. {@code https://backup-account.blob.core.windows.net}
     */
    public ReadFallbackPolicy(String secondaryBlobEndpoint) {
        this.secondaryBlobEndpoint = secondaryBlobEndpoint;
    }

    @Override
    public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next) {
        HttpRequest request = context.getHttpRequest();

        if (request.getHttpMethod() != HttpMethod.GET && request.getHttpMethod() != HttpMethod.HEAD) {
            return next.process();
        }

        URL originalUrl = request.getUrl();

        // clone before consuming so we can replay the pipeline for the fallback
        HttpPipelineNextPolicy nextForFallback = next.clone();

        return next.process().flatMap(response -> {
            if (response.getStatusCode() != 404) {
                return Mono.just(response);
            }

            URL fallbackUrl;
            try {
                fallbackUrl = buildFallbackUrl(originalUrl);
            } catch (MalformedURLException e) {
                log.warn("Failed to build fallback URL for {}: {}", originalUrl, e.getMessage());
                return Mono.just(response);
            }

            log.debug("Primary returned 404 for {}, falling back to {}", originalUrl, fallbackUrl);

            request.setUrl(fallbackUrl);

            return nextForFallback.process().doOnNext(fallbackResponse -> {
                // restore original URL regardless of outcome
                request.setUrl(originalUrl);
                if (fallbackResponse.getStatusCode() == 404) {
                    log.debug("Secondary also returned 404 for {}", fallbackUrl);
                }
            });
        });
    }

    private URL buildFallbackUrl(URL originalUrl) throws MalformedURLException {
        URL secondary;
        try {
            secondary = new URL(secondaryBlobEndpoint);
        } catch (MalformedURLException e) {
            throw new MalformedURLException("Invalid secondary blob endpoint: " + secondaryBlobEndpoint);
        }

        return new URL(
                secondary.getProtocol(),
                secondary.getHost(),
                secondary.getPort(),
                originalUrl.getFile()  // preserves path + query string
        );
    }
}
