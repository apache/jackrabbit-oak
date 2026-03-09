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
import java.util.concurrent.ExecutionException;

/**
 * Unit cases for {@link FutureUtils}
 */
public class FutureUtilsTest {

    @Test
    public void successfulAsListAllSuccessful() throws Exception {
        CompletableFuture<String> cf1 = CompletableFuture.completedFuture("a");
        CompletableFuture<String> cf2 = CompletableFuture.completedFuture("b");
        List<CompletableFuture<String>> futures = Arrays.asList(cf1, cf2);

        List<String> result = FutureUtils.successfulAsList(futures).get();

        Assert.assertEquals(Arrays.asList("a", "b"), result);
    }

    @Test
    public void successfulAsListPartialFailure() throws Exception {
        CompletableFuture<String> cf1 = CompletableFuture.completedFuture("a");
        CompletableFuture<String> cf2 = new CompletableFuture<>();
        cf2.completeExceptionally(new RuntimeException("fail"));
        List<CompletableFuture<String>> futures = Arrays.asList(cf1, cf2);

        List<String> result = FutureUtils.successfulAsList(futures).get();

        Assert.assertEquals(Arrays.asList("a", null), result);
    }

    @Test
    public void successfulAsListAllFailures() throws Exception {
        CompletableFuture<String> cf1 = new CompletableFuture<>();
        cf1.completeExceptionally(new RuntimeException("fail A"));
        CompletableFuture<String> cf2 = new CompletableFuture<>();
        cf2.completeExceptionally(new RuntimeException("fail B"));
        List<CompletableFuture<String>> futures = Arrays.asList(cf1, cf2);

        List<String> result = FutureUtils.successfulAsList(futures).get();

        Assert.assertEquals(Arrays.asList(null, null), result);
    }

    @Test
    public void successfulAsListEmptyList() throws Exception {
        List<Object> result = FutureUtils.successfulAsList(List.of()).get();

        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void allAsListAllSuccessful() throws Exception {
        CompletableFuture<String> cf1 = CompletableFuture.completedFuture("foo");
        CompletableFuture<String> cf2 = CompletableFuture.completedFuture("bar");
        List<CompletableFuture<String>> futures = Arrays.asList(cf1, cf2);

        List<String> result = FutureUtils.allAsList(futures).get();

        Assert.assertEquals(Arrays.asList("foo", "bar"), result);
    }

    @Test
    public void allAsListPartialFailure() throws Exception {
        CompletableFuture<String> cf1 = CompletableFuture.completedFuture("ok");
        CompletableFuture<String> cf2 = new CompletableFuture<>();
        cf2.completeExceptionally(new IllegalStateException("fail"));
        List<CompletableFuture<String>> futures = Arrays.asList(cf1, cf2);

        boolean failed;
        try {
            FutureUtils.allAsList(futures).get();
            failed = false;
        } catch (ExecutionException e) {
            failed = true;
        }

        Assert.assertTrue(failed);
    }

    @Test
    public void allAsListAllFailures() throws Exception {
        CompletableFuture<String> cf1 = new CompletableFuture<>();
        cf1.completeExceptionally(new RuntimeException("f1 failed"));
        CompletableFuture<String> cf2 = new CompletableFuture<>();
        cf2.completeExceptionally(new RuntimeException("f2 failed"));
        List<CompletableFuture<String>> futures = Arrays.asList(cf1, cf2);

        boolean failed;
        try {
            FutureUtils.allAsList(futures).get();
            failed = false;
        } catch (ExecutionException e) {
            failed = true;
        }

        Assert.assertTrue(failed);
    }

    @Test
    public void allAsListEmptyList() throws Exception {
        List<Object> result = FutureUtils.allAsList(List.of()).get();

        Assert.assertTrue(result.isEmpty());
    }

}