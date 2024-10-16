package org.apache.jackrabbit.oak.segment.azure;

import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.HttpPipelinePolicy;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;

public class AzureHttpRequestLoggingTestingPolicy implements HttpPipelinePolicy {

    private static final Logger log = LoggerFactory.getLogger(AzureHttpRequestLoggingTestingPolicy.class);

    @Override
    public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next) {
        Stopwatch stopwatch = Stopwatch.createStarted();

        log.info("HTTP Request: {} {}", context.getHttpRequest().getHttpMethod(), context.getHttpRequest().getUrl());

        return next.process().flatMap(httpResponse -> {
            log.info("Status code is: {}", httpResponse.getStatusCode());
            log.info("Response time: {}ms", (stopwatch.elapsed(TimeUnit.NANOSECONDS)) / 1_000_000);

            return Mono.just(httpResponse);
        });
    }
}
