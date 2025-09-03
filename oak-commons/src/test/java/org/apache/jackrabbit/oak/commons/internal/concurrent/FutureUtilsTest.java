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
package org.apache.jackrabbit.oak.commons.internal.concurrent;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Unit cases for {@link FutureUtils}
 */
public class FutureUtilsTest {

    @Test
    public void testAllSuccessful() throws Exception {
        CompletableFuture<String> f1 = CompletableFuture.completedFuture("a");
        CompletableFuture<String> f2 = CompletableFuture.completedFuture("b");

        List<CompletableFuture<String>> futures = Arrays.asList(f1, f2);
        CompletableFuture<List<String>> combined = FutureUtils.successfulAsList(futures);

        List<String> results = combined.get();
        Assert.assertEquals(2, results.size());
        Assert.assertEquals("a", results.get(0));
        Assert.assertEquals("b", results.get(1));
    }

    @Test
    public void testPartialFailure() throws Exception {
        CompletableFuture<String> f1 = CompletableFuture.completedFuture("a");
        CompletableFuture<String> f2 = new CompletableFuture<>();
        f2.completeExceptionally(new RuntimeException("fail"));

        List<CompletableFuture<String>> futures = Arrays.asList(f1, f2);
        CompletableFuture<List<String>> combined = FutureUtils.successfulAsList(futures);

        List<String> results = combined.get();
        Assert.assertEquals(2, results.size());
        Assert.assertEquals("a", results.get(0));
        Assert.assertNull(results.get(1));  // Failure replaced by null
    }

    @Test
    public void testAllFailures() throws Exception {
        CompletableFuture<String> f1 = new CompletableFuture<>();
        f1.completeExceptionally(new RuntimeException("fail A"));

        CompletableFuture<String> f2 = new CompletableFuture<>();
        f2.completeExceptionally(new RuntimeException("fail B"));

        List<CompletableFuture<String>> futures = Arrays.asList(f1, f2);
        CompletableFuture<List<String>> combined = FutureUtils.successfulAsList(futures);

        List<String> results = combined.get();
        Assert.assertEquals(2, results.size());
        Assert.assertNull(results.get(0));
        Assert.assertNull(results.get(1));
    }

    @Test
    public void testEmptyList() throws Exception {
        List<CompletableFuture<String>> futures = List.of();
        CompletableFuture<List<String>> combined = FutureUtils.successfulAsList(futures);

        List<String> results = combined.get();
        Assert.assertTrue(results.isEmpty());
    }

}