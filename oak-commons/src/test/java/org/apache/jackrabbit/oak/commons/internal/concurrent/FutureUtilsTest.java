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

import org.apache.jackrabbit.guava.common.util.concurrent.Futures;
import org.apache.jackrabbit.guava.common.util.concurrent.ListenableFuture;
import org.apache.jackrabbit.guava.common.util.concurrent.SettableFuture;
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
        List<CompletableFuture<String>> jdkFutures = Arrays.asList(cf1, cf2);

        SettableFuture<String> lf1 = SettableFuture.create();
        SettableFuture<String> lf2 = SettableFuture.create();
        lf1.set("a");
        lf2.set("b");
        List<ListenableFuture<String>> guavaFutures = Arrays.asList(lf1, lf2);

        // Native JDK method
        List<String> jdkResults = FutureUtils.successfulAsList(jdkFutures).get();

        // Guava method (blocking)
        List<String> guavaResults = Futures.successfulAsList(guavaFutures).get();

        Assert.assertEquals(guavaResults, jdkResults);
    }

    @Test
    public void successfulAsListPartialFailure() throws Exception {
        CompletableFuture<String> cf1 = CompletableFuture.completedFuture("a");
        CompletableFuture<String> cf2 = new CompletableFuture<>();
        cf2.completeExceptionally(new RuntimeException("fail"));
        List<CompletableFuture<String>> jdkFutures = Arrays.asList(cf1, cf2);

        SettableFuture<String> lf1 = SettableFuture.create();
        SettableFuture<String> lf2 = SettableFuture.create();
        lf1.set("a");
        lf2.setException(new RuntimeException("fail"));
        List<ListenableFuture<String>> guavaFutures = Arrays.asList(lf1, lf2);

        // Native JDK method
        List<String> jdkResults = FutureUtils.successfulAsList(jdkFutures).get();

        // Guava method (blocking)
        List<String> guavaResults = Futures.successfulAsList(guavaFutures).get();

        Assert.assertEquals(guavaResults, jdkResults);
    }

    @Test
    public void successfulAsListAllFailures() throws Exception {
        CompletableFuture<String> cf1 = new CompletableFuture<>();
        cf1.completeExceptionally(new RuntimeException("fail A"));
        CompletableFuture<String> cf2 = new CompletableFuture<>();
        cf2.completeExceptionally(new RuntimeException("fail B"));
        List<CompletableFuture<String>> jdkFutures = Arrays.asList(cf1, cf2);

        SettableFuture<String> lf1 = SettableFuture.create();
        SettableFuture<String> lf2 = SettableFuture.create();
        lf1.setException(new RuntimeException("fail A"));
        lf2.setException(new RuntimeException("fail B"));
        List<ListenableFuture<String>> guavaFutures = Arrays.asList(lf1, lf2);

        // Native JDK method
        List<String> jdkResults = FutureUtils.successfulAsList(jdkFutures).get();

        // Guava method (blocking)
        List<String> guavaResults = Futures.successfulAsList(guavaFutures).get();

        Assert.assertEquals(guavaResults, jdkResults);
    }

    @Test
    public void successfulAsListEmptyList() throws Exception {
        List<CompletableFuture<String>> jdkFutures = List.of();
        List<ListenableFuture<String>> guavaFutures = List.of();

        List<String> jdkResult = FutureUtils.successfulAsList(jdkFutures).get();
        List<String> guavaResult = Futures.successfulAsList(guavaFutures).get();

        Assert.assertEquals(guavaResult, jdkResult);
        Assert.assertTrue(jdkResult.isEmpty());
    }

    @Test
    public void allAsListAllSuccessful() throws Exception {
        CompletableFuture<String> cf1 = CompletableFuture.completedFuture("foo");
        CompletableFuture<String> cf2 = CompletableFuture.completedFuture("bar");
        List<CompletableFuture<String>> jdkFutures = Arrays.asList(cf1, cf2);

        SettableFuture<String> lf1 = SettableFuture.create();
        SettableFuture<String> lf2 = SettableFuture.create();
        lf1.set("foo");
        lf2.set("bar");
        List<ListenableFuture<String>> guavaFutures = Arrays.asList(lf1, lf2);

        // Native JDK method
        List<String> jdkResults = FutureUtils.allAsList(jdkFutures).get();

        // Guava method (blocking)
        List<String> guavaResults = Futures.allAsList(guavaFutures).get();

        Assert.assertEquals(guavaResults, jdkResults);
    }

    @Test
    public void allAsListPartialFailure() throws Exception {
        CompletableFuture<String> cf1 = CompletableFuture.completedFuture("ok");
        CompletableFuture<String> cf2 = new CompletableFuture<>();
        cf2.completeExceptionally(new IllegalStateException("fail"));
        List<CompletableFuture<String>> jdkFutures = Arrays.asList(cf1, cf2);

        SettableFuture<String> lf1 = SettableFuture.create();
        SettableFuture<String> lf2 = SettableFuture.create();
        lf1.set("ok");
        lf2.setException(new IllegalStateException("fail"));
        List<ListenableFuture<String>> guavaFutures = Arrays.asList(lf1, lf2);

        boolean jdkFailed;
        try {
            FutureUtils.allAsList(jdkFutures).get();
            jdkFailed = false;
        } catch (ExecutionException e) {
            jdkFailed = true;
        }

        boolean guavaFailed;
        try {
            Futures.allAsList(guavaFutures).get();
            guavaFailed = false;
        } catch (ExecutionException e) {
            guavaFailed = true;
        }

        Assert.assertTrue(jdkFailed);
        Assert.assertTrue(guavaFailed);
    }

    @Test
    public void allAsListAllFailures() throws Exception {
        CompletableFuture<String> cf1 = new CompletableFuture<>();
        cf1.completeExceptionally(new RuntimeException("f1 failed"));
        CompletableFuture<String> cf2 = new CompletableFuture<>();
        cf2.completeExceptionally(new RuntimeException("f2 failed"));
        List<CompletableFuture<String>> jdkFutures = Arrays.asList(cf1, cf2);

        SettableFuture<String> lf1 = SettableFuture.create();
        SettableFuture<String> lf2 = SettableFuture.create();
        lf1.setException(new RuntimeException("f1 failed"));
        lf2.setException(new RuntimeException("f2 failed"));
        List<ListenableFuture<String>> guavaFutures = Arrays.asList(lf1, lf2);

        boolean jdkFailed;
        try {
            FutureUtils.allAsList(jdkFutures).get();
            jdkFailed = false;
        } catch (ExecutionException e) {
            jdkFailed = true;
        }

        boolean guavaFailed;
        try {
            Futures.allAsList(guavaFutures).get();
            guavaFailed = false;
        } catch (ExecutionException e) {
            guavaFailed = true;
        }

        Assert.assertTrue(jdkFailed);
        Assert.assertTrue(guavaFailed);
    }

    @Test
    public void allAsListEmptyList() throws Exception {
        List<CompletableFuture<String>> jdkFutures = List.of();
        List<ListenableFuture<String>> guavaFutures = List.of();

        List<String> jdkResult = FutureUtils.allAsList(jdkFutures).get();
        List<String> guavaResult = Futures.allAsList(guavaFutures).get();

        Assert.assertTrue(jdkResult.isEmpty());
        Assert.assertTrue(guavaResult.isEmpty());
    }

}