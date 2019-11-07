/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
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
import io.netty.handler.codec.http.HttpStatusClass;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;

/**
 * HttpPipelinePolicy is what we use to hook into the network events.
 * It gives access to outgoing requests and incoming responses/errors. The metrics are reported
 * to an instance of {@link RemoteStoreMonitor}
 */
public class AzureStorageMonitorPolicy implements HttpPipelinePolicy {
    private RemoteStoreMonitor monitor;

    public AzureStorageMonitorPolicy setMonitor(@NotNull final RemoteStoreMonitor monitor) {
        this.monitor = monitor;
        return this;
    }

    @Override
    public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next) {
        if(this.monitor==null) {
            return next.process();
        }
        long start = System.currentTimeMillis();
        return next.process()
                // OnSuccess also includes 5xx responses (which did not throw an exception)
                .doOnSuccess(e -> {
                    HttpStatusClass httpStatusClass = HttpStatusClass.valueOf(e.getStatusCode());
                    if (httpStatusClass == HttpStatusClass.SUCCESS || httpStatusClass == HttpStatusClass.REDIRECTION) {
                        handleSuccess(start);
                    } else if (httpStatusClass == HttpStatusClass.CLIENT_ERROR || httpStatusClass == HttpStatusClass.SERVER_ERROR) {
                        handleError(start);
                    }
                    // ignore other informational codes .
                })
                // doOnError handles exceptions thrown from the httpclient.
                .doOnError(e -> handleError(start));
    }

    public void handleSuccess(long start) {
        monitor.requestCount();
        monitor.requestDuration(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
    }

    public void handleError(long start) {
        monitor.requestError();
        monitor.requestDuration(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
    }
}
