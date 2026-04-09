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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import reactor.core.publisher.Mono;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class AzureHttpRequestLoggingPolicyV12Test {

    private String originalVerboseProperty;

    @Before
    public void setUp() {
        // Save the original system property value
        originalVerboseProperty = System.getProperty("blob.azure.v12.http.verbose.enabled");
    }

    @After
    public void tearDown() {
        // Restore the original system property value
        if (originalVerboseProperty != null) {
            System.setProperty("blob.azure.v12.http.verbose.enabled", originalVerboseProperty);
        } else {
            System.clearProperty("blob.azure.v12.http.verbose.enabled");
        }
    }

    @Test
    public void testLoggingPolicyCreation() {
        AzureHttpRequestLoggingPolicyV12 policy = new AzureHttpRequestLoggingPolicyV12();
        assertNotNull("Logging policy should be created successfully", policy);
    }

    @Test
    public void testProcessRequestWithVerboseDisabled() throws MalformedURLException {
        // Ensure verbose logging is disabled
        System.clearProperty("blob.azure.v12.http.verbose.enabled");

        AzureHttpRequestLoggingPolicyV12 policy = new AzureHttpRequestLoggingPolicyV12();

        // Mock the context and request
        HttpPipelineCallContext context = mock(HttpPipelineCallContext.class);
        HttpRequest request = mock(HttpRequest.class);
        HttpResponse response = mock(HttpResponse.class);
        HttpPipelineNextPolicy nextPolicy = mock(HttpPipelineNextPolicy.class);

        // Set up mocks
        when(context.getHttpRequest()).thenReturn(request);
        when(request.getHttpMethod()).thenReturn(HttpMethod.GET);
        when(request.getUrl()).thenReturn(new URL("https://test.blob.core.windows.net/container/blob"));
        when(response.getStatusCode()).thenReturn(200);
        when(nextPolicy.process()).thenReturn(Mono.just(response));

        // Execute the policy
        Mono<HttpResponse> result = policy.process(context, nextPolicy);

        // Verify the result
        assertNotNull("Policy should return a response", result);
        try (HttpResponse actualResponse = result.block()) {
            assertNotNull("Response should not be null", actualResponse);
            assertEquals("Response should have correct status code", 200, actualResponse.getStatusCode());
        }

        // Verify that the next policy was called
        verify(nextPolicy, times(1)).process();
    }

    @Test
    public void testProcessRequestWithVerboseEnabled() throws MalformedURLException {
        // Enable verbose logging
        System.setProperty("blob.azure.v12.http.verbose.enabled", "true");

        AzureHttpRequestLoggingPolicyV12 policy = new AzureHttpRequestLoggingPolicyV12();

        // Mock the context and request
        HttpPipelineCallContext context = mock(HttpPipelineCallContext.class);
        HttpRequest request = mock(HttpRequest.class);
        HttpResponse response = mock(HttpResponse.class);
        HttpPipelineNextPolicy nextPolicy = mock(HttpPipelineNextPolicy.class);

        // Set up mocks
        when(context.getHttpRequest()).thenReturn(request);
        when(request.getHttpMethod()).thenReturn(HttpMethod.POST);
        when(request.getUrl()).thenReturn(new URL("https://test.blob.core.windows.net/container/blob"));
        when(response.getStatusCode()).thenReturn(201);
        when(nextPolicy.process()).thenReturn(Mono.just(response));

        // Execute the policy
        Mono<HttpResponse> result = policy.process(context, nextPolicy);

        // Verify the result
        assertNotNull("Policy should return a response", result);
        try (HttpResponse actualResponse = result.block()) {
            assertNotNull("Response should not be null", actualResponse);
            assertEquals("Response should have correct status code", 201, actualResponse.getStatusCode());
        }

        // Verify that the next policy was called
        verify(nextPolicy, times(1)).process();
        // Verify that context.getHttpRequest() was called for logging
        verify(context, atLeastOnce()).getHttpRequest();
    }

    @Test
    public void testProcessRequestWithDifferentHttpMethods() throws MalformedURLException {
        System.setProperty("blob.azure.v12.http.verbose.enabled", "true");

        AzureHttpRequestLoggingPolicyV12 policy = new AzureHttpRequestLoggingPolicyV12();

        // Test different HTTP methods
        HttpMethod[] methods = {HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, HttpMethod.DELETE, HttpMethod.HEAD};

        for (HttpMethod method : methods) {
            // Mock the context and request
            HttpPipelineCallContext context = mock(HttpPipelineCallContext.class);
            HttpRequest request = mock(HttpRequest.class);
            HttpResponse response = mock(HttpResponse.class);
            HttpPipelineNextPolicy nextPolicy = mock(HttpPipelineNextPolicy.class);

            // Set up mocks
            when(context.getHttpRequest()).thenReturn(request);
            when(request.getHttpMethod()).thenReturn(method);
            when(request.getUrl()).thenReturn(new URL("https://test.blob.core.windows.net/container/blob"));
            when(response.getStatusCode()).thenReturn(200);
            when(nextPolicy.process()).thenReturn(Mono.just(response));

            // Execute the policy
            Mono<HttpResponse> result = policy.process(context, nextPolicy);

            // Verify the result
            assertNotNull("Policy should return a response for " + method, result);
            try (HttpResponse actualResponse = result.block()) {
                assertNotNull("Response should not be null for " + method, actualResponse);
                assertEquals("Response should have correct status code for " + method, 200, actualResponse.getStatusCode());
            }
        }
    }

    @Test
    public void testProcessRequestWithDifferentStatusCodes() throws MalformedURLException {
        System.setProperty("blob.azure.v12.http.verbose.enabled", "true");

        AzureHttpRequestLoggingPolicyV12 policy = new AzureHttpRequestLoggingPolicyV12();

        // Test different status codes
        int[] statusCodes = {200, 201, 204, 400, 404, 500};

        for (int statusCode : statusCodes) {
            // Mock the context and request
            HttpPipelineCallContext context = mock(HttpPipelineCallContext.class);
            HttpRequest request = mock(HttpRequest.class);
            HttpResponse response = mock(HttpResponse.class);
            HttpPipelineNextPolicy nextPolicy = mock(HttpPipelineNextPolicy.class);

            // Set up mocks
            when(context.getHttpRequest()).thenReturn(request);
            when(request.getHttpMethod()).thenReturn(HttpMethod.GET);
            when(request.getUrl()).thenReturn(new URL("https://test.blob.core.windows.net/container/blob"));
            when(response.getStatusCode()).thenReturn(statusCode);
            when(nextPolicy.process()).thenReturn(Mono.just(response));

            // Execute the policy
            Mono<HttpResponse> result = policy.process(context, nextPolicy);

            // Verify the result
            assertNotNull("Policy should return a response for status " + statusCode, result);
            try (HttpResponse actualResponse = result.block()) {
                assertNotNull("Response should not be null for status " + statusCode, actualResponse);
                assertEquals("Response should have correct status code", statusCode, actualResponse.getStatusCode());
            }
        }
    }

    @Test
    public void testProcessRequestWithErrorInNextPolicy() throws MalformedURLException {
        System.setProperty("blob.azure.v12.http.verbose.enabled", "true");

        AzureHttpRequestLoggingPolicyV12 policy = new AzureHttpRequestLoggingPolicyV12();

        // Mock the context and request
        HttpPipelineCallContext context = mock(HttpPipelineCallContext.class);
        HttpRequest request = mock(HttpRequest.class);
        HttpPipelineNextPolicy nextPolicy = mock(HttpPipelineNextPolicy.class);

        // Set up mocks
        when(context.getHttpRequest()).thenReturn(request);
        when(request.getHttpMethod()).thenReturn(HttpMethod.GET);
        when(request.getUrl()).thenReturn(new URL("https://test.blob.core.windows.net/container/blob"));

        // Simulate an error in the next policy
        RuntimeException testException = new RuntimeException("Test exception");
        when(nextPolicy.process()).thenReturn(Mono.error(testException));

        // Execute the policy
        Mono<HttpResponse> result = policy.process(context, nextPolicy);

        // Verify that the error is propagated
        try (HttpResponse httpResponse = result.block()) {
              fail("Expected exception to be thrown");
        } catch (RuntimeException e) {
            assertEquals("Exception should be propagated", testException, e);
        }

        // Verify that the next policy was called
        verify(nextPolicy, times(1)).process();
    }

    @Test
    public void testProcessRequestWithVerbosePropertySetToFalse() throws MalformedURLException {
        // Explicitly set verbose logging to false
        System.setProperty("blob.azure.v12.http.verbose.enabled", "false");

        AzureHttpRequestLoggingPolicyV12 policy = new AzureHttpRequestLoggingPolicyV12();

        // Mock the context and request
        HttpPipelineCallContext context = mock(HttpPipelineCallContext.class);
        HttpRequest request = mock(HttpRequest.class);
        HttpResponse response = mock(HttpResponse.class);
        HttpPipelineNextPolicy nextPolicy = mock(HttpPipelineNextPolicy.class);

        // Set up mocks
        when(context.getHttpRequest()).thenReturn(request);
        when(request.getHttpMethod()).thenReturn(HttpMethod.GET);
        when(request.getUrl()).thenReturn(new URL("https://test.blob.core.windows.net/container/blob"));
        when(response.getStatusCode()).thenReturn(200);
        when(nextPolicy.process()).thenReturn(Mono.just(response));

        // Execute the policy
        Mono<HttpResponse> result = policy.process(context, nextPolicy);

        // Verify the result
        assertNotNull("Policy should return a response", result);
        try (HttpResponse actualResponse = result.block()) {
            assertNotNull("Response should not be null", actualResponse);
            assertEquals("Response should have correct status code", 200, actualResponse.getStatusCode());
        }

        // Verify that the next policy was called
        verify(nextPolicy, times(1)).process();
    }

    @Test
    public void testProcessRequestWithComplexUrl() throws MalformedURLException {
        System.setProperty("blob.azure.v12.http.verbose.enabled", "true");

        AzureHttpRequestLoggingPolicyV12 policy = new AzureHttpRequestLoggingPolicyV12();

        // Mock the context and request
        HttpPipelineCallContext context = mock(HttpPipelineCallContext.class);
        HttpRequest request = mock(HttpRequest.class);
        HttpResponse response = mock(HttpResponse.class);
        HttpPipelineNextPolicy nextPolicy = mock(HttpPipelineNextPolicy.class);

        // Set up mocks with a complex URL
        URL complexUrl = new URL("https://mystorageaccount.blob.core.windows.net/mycontainer/path/to/blob?sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupx&se=2021-12-31T23:59:59Z&st=2021-01-01T00:00:00Z&spr=https,http&sig=signature");
        when(context.getHttpRequest()).thenReturn(request);
        when(request.getHttpMethod()).thenReturn(HttpMethod.PUT);
        when(request.getUrl()).thenReturn(complexUrl);
        when(response.getStatusCode()).thenReturn(201);
        when(nextPolicy.process()).thenReturn(Mono.just(response));

        // Execute the policy
        Mono<HttpResponse> result = policy.process(context, nextPolicy);

        // Verify the result
        assertNotNull("Policy should return a response", result);
        try (HttpResponse actualResponse = result.block()) {
            assertNotNull("Response should not be null", actualResponse);
            assertEquals("Response should have correct status code", 201, actualResponse.getStatusCode());
        }

        // Verify that the next policy was called
        verify(nextPolicy, times(1)).process();
        // Verify that context.getHttpRequest() was called for logging
        verify(context, atLeastOnce()).getHttpRequest();
    }

    @Test
    public void testProcessRequestWithNullContext() {
        AzureHttpRequestLoggingPolicyV12 policy = new AzureHttpRequestLoggingPolicyV12();
        HttpPipelineNextPolicy nextPolicy = mock(HttpPipelineNextPolicy.class);
        HttpResponse response = mock(HttpResponse.class);

        when(response.getStatusCode()).thenReturn(200);
        when(nextPolicy.process()).thenReturn(Mono.just(response));

        try {
            Mono<HttpResponse> result = policy.process(null, nextPolicy);
            // May succeed with null context - policy might handle it gracefully
            if (result != null) {
                try (HttpResponse actualResponse = result.block()) {
                    assertNotNull("Response should not be null", actualResponse);
                }
            }
        } catch (Exception e) {
            // Expected - may fail with null context
            assertNotNull("Exception should not be null", e);
        }
    }

    @Test
    public void testProcessRequestWithNullNextPolicy() {
        AzureHttpRequestLoggingPolicyV12 policy = new AzureHttpRequestLoggingPolicyV12();
        HttpPipelineCallContext context = mock(HttpPipelineCallContext.class);
        HttpRequest request = mock(HttpRequest.class);

        when(context.getHttpRequest()).thenReturn(request);
        when(request.getHttpMethod()).thenReturn(HttpMethod.GET);

        try {
            policy.process(context, null);
            fail("Expected exception with null next policy");
        } catch (Exception e) {
            // Expected - should handle null next policy
            assertNotNull("Exception should not be null", e);
        }
    }

    @Test
    public void testProcessRequestWithSlowResponse() throws MalformedURLException {
        System.setProperty("blob.azure.v12.http.verbose.enabled", "true");

        AzureHttpRequestLoggingPolicyV12 policy = new AzureHttpRequestLoggingPolicyV12();

        HttpPipelineCallContext context = mock(HttpPipelineCallContext.class);
        HttpRequest request = mock(HttpRequest.class);
        HttpResponse response = mock(HttpResponse.class);
        HttpPipelineNextPolicy nextPolicy = mock(HttpPipelineNextPolicy.class);

        when(context.getHttpRequest()).thenReturn(request);
        when(request.getHttpMethod()).thenReturn(HttpMethod.GET);
        when(request.getUrl()).thenReturn(new URL("https://test.blob.core.windows.net/container/blob"));
        when(response.getStatusCode()).thenReturn(200);

        // Simulate slow response
        when(nextPolicy.process()).thenReturn(Mono.just(response).delayElement(Duration.ofMillis(100)));

        Mono<HttpResponse> result = policy.process(context, nextPolicy);

        // Verify the result
        assertNotNull("Policy should return a response", result);
        try (HttpResponse actualResponse = result.block()) {
            assertNotNull("Response should not be null", actualResponse);
            assertEquals("Response should have correct status code", 200, actualResponse.getStatusCode());
        }
    }

    @Test
    public void testVerboseLoggingSystemPropertyDetection() {
        // Test with different system property values
        String[] testValues = {"true", "TRUE", "True", "false", "FALSE", "False", "invalid", ""};

        for (String value : testValues) {
            System.setProperty("blob.azure.v12.http.verbose.enabled", value);
            AzureHttpRequestLoggingPolicyV12 policy = new AzureHttpRequestLoggingPolicyV12();
            assertNotNull("Policy should be created regardless of system property value", policy);
        }
    }
}
