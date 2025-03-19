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
package org.apache.jackrabbit.oak.segment.azure;

import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.HttpPipelinePolicy;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;

public class AzureHttpRequestLoggingPolicy implements HttpPipelinePolicy {

    private static final Logger log = LoggerFactory.getLogger(AzureHttpRequestLoggingPolicy.class);

    private final boolean verboseEnabled = Boolean.getBoolean("segment.azure.v12.http.verbose.enabled");

    private RemoteStoreMonitor remoteStoreMonitor;

    public void setRemoteStoreMonitor(RemoteStoreMonitor remoteStoreMonitor) {
        log.info("Enable Azure Remote store Monitor");
        this.remoteStoreMonitor = remoteStoreMonitor;
    }

    @Override
    public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next) {
        Stopwatch stopwatch = Stopwatch.createStarted();

        return next.process().flatMap(httpResponse -> {
            if (remoteStoreMonitor != null) {
                remoteStoreMonitor.requestDuration(stopwatch.elapsed(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);
                if (httpResponse.getStatusCode() > 201) {
                    remoteStoreMonitor.requestError();
                } else {
                    remoteStoreMonitor.requestCount();
                }
            }

            if (verboseEnabled) {
                log.info("HTTP Request: {} {} {} {}ms", context.getHttpRequest().getHttpMethod(), context.getHttpRequest().getUrl(), httpResponse.getStatusCode(), (stopwatch.elapsed(TimeUnit.NANOSECONDS))/1_000_000);
            }

            return Mono.just(httpResponse);
        });
    }

}
