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

import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.HttpPipelinePolicy;

import org.apache.jackrabbit.oak.commons.properties.SystemPropertySupplier;
import org.apache.jackrabbit.oak.commons.time.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;

/**
 * HTTP pipeline policy for logging Azure Blob Storage requests in the oak-blob-cloud-azure module.
 *
 * This policy logs HTTP request details including method, URL, status code, and duration.
 * Verbose logging can be enabled by setting the system property:
 * -Dblob.azure.v12.http.verbose.enabled=true
 *
 * This is similar to the AzureHttpRequestLoggingPolicyV12 in oak-segment-azure but specifically
 * designed for the blob storage operations in oak-blob-cloud-azure.
 */
public class AzureHttpRequestLoggingPolicyV12 implements HttpPipelinePolicy {

    private static final Logger log = LoggerFactory.getLogger(AzureHttpRequestLoggingPolicyV12.class);

    private static final String AZURE_SDK_VERBOSE_LOGGING_ENABLED = "blob.azure.v12.http.verbose.enabled";

    private final boolean verboseEnabled = SystemPropertySupplier.create(AZURE_SDK_VERBOSE_LOGGING_ENABLED, false).get();

    @Override
    public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next) {
        Stopwatch stopwatch = Stopwatch.createStarted();

        return next.process().flatMap(httpResponse -> {
            if (verboseEnabled) {
                log.info("HTTP Blob Request: {} {} {} {} ms",
                    context.getHttpRequest().getHttpMethod(), 
                    context.getHttpRequest().getUrl(), 
                    httpResponse.getStatusCode(), 
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));
            }

            return Mono.just(httpResponse);
        });
    }
}
