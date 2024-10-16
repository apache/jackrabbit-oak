package org.apache.jackrabbit.oak.segment.azure;

import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

public class AzureHttpRequestLoggingPolicyTest {

    private AzureHttpRequestLoggingPolicy loggingPolicy;
    private HttpPipelineCallContext mockContext;
    private HttpPipelineNextPolicy mockNextPolicy;
    private HttpResponse mockHttpResponse;
    private RemoteStoreMonitor mockRemoteStoreMonitor;

    @Before
    public void setup() {
        loggingPolicy = new AzureHttpRequestLoggingPolicy();
        mockContext = mock(HttpPipelineCallContext.class);
        mockNextPolicy = mock(HttpPipelineNextPolicy.class);
        mockHttpResponse = mock(HttpResponse.class);
        mockRemoteStoreMonitor = mock(RemoteStoreMonitor.class);
    }


    @Test
    public void testRemoteStoreMonitorTracksMetrics() {
        // Attach the remote store monitor
        loggingPolicy.setRemoteStoreMonitor(mockRemoteStoreMonitor);

        // Setup mock behavior
        HttpRequest mockHttpRequest = mock(HttpRequest.class);
        when(mockContext.getHttpRequest()).thenReturn(mockHttpRequest);
        when(mockNextPolicy.process()).thenReturn(Mono.just(mockHttpResponse));
        when(mockHttpResponse.getStatusCode()).thenReturn(200);

        // Run the process method
        Mono<HttpResponse> result = loggingPolicy.process(mockContext, mockNextPolicy);

        // Verify the result
        StepVerifier.create(result)
                .expectNext(mockHttpResponse)
                .verifyComplete();

        // Verify that the monitor recorded the metrics
        verify(mockRemoteStoreMonitor, times(1)).requestDuration(anyLong(), eq(TimeUnit.NANOSECONDS));
        verify(mockRemoteStoreMonitor, times(1)).requestCount();
        verify(mockRemoteStoreMonitor, never()).requestError();
    }

    @Test
    public void testErrorStatusCodeTriggersErrorCount() {
        loggingPolicy.setRemoteStoreMonitor(mockRemoteStoreMonitor);

        // Setup mock behavior
        HttpRequest mockHttpRequest = mock(HttpRequest.class);
        when(mockContext.getHttpRequest()).thenReturn(mockHttpRequest);
        when(mockNextPolicy.process()).thenReturn(Mono.just(mockHttpResponse));
        when(mockHttpResponse.getStatusCode()).thenReturn(500);  // Error status code

        // Run the process method
        Mono<HttpResponse> result = loggingPolicy.process(mockContext, mockNextPolicy);

        // Verify the result
        StepVerifier.create(result)
                .expectNext(mockHttpResponse)
                .verifyComplete();

        // Verify that error count was recorded
        verify(mockRemoteStoreMonitor, times(1)).requestDuration(anyLong(), eq(TimeUnit.NANOSECONDS));
        verify(mockRemoteStoreMonitor, times(1)).requestError();
        verify(mockRemoteStoreMonitor, never()).requestCount();
    }

    @Test
    public void testNoRemoteStoreMonitor() {
        // Setup: No remoteStoreMonitor is attached
        when(mockNextPolicy.process()).thenReturn(Mono.just(mockHttpResponse));
        when(mockHttpResponse.getStatusCode()).thenReturn(200);

        // Run the process method
        Mono<HttpResponse> result = loggingPolicy.process(mockContext, mockNextPolicy);

        // Verify that the result is correct and that no interactions with the monitor occurred
        StepVerifier.create(result)
                .expectNext(mockHttpResponse)
                .verifyComplete();

        verifyNoInteractions(mockRemoteStoreMonitor);
    }
}
