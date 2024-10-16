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
                log.info("HTTP Request: {} {}", context.getHttpRequest().getHttpMethod(), context.getHttpRequest().getUrl());
                log.info("Status code is: {}", httpResponse.getStatusCode());
                log.info("Response time: {}ms", (stopwatch.elapsed(TimeUnit.NANOSECONDS))/1_000_000);
            }

            return Mono.just(httpResponse);
        });
    }

}
