/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.cache;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Tests for CacheChangesTracker concurrency scenarios, particularly
 * the LazyBloomFilter double-checked locking implementation.
 */
public class CacheChangesTrackerConcurrencyTest {

    /**
     * Test concurrent initialization of LazyBloomFilter to ensure
     * double-checked locking prevents race conditions.
     */
    @Test
    public void testLazyBloomFilterConcurrentInitialization() throws InterruptedException {
        final int threadCount = 20;
        final int entriesPerThread = 50;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch doneLatch = new CountDownLatch(threadCount);
        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        
        // Create a LazyBloomFilter instance
        final CacheChangesTracker.LazyBloomFilter lazyFilter = 
            new CacheChangesTracker.LazyBloomFilter(1000);
        
        final AtomicInteger putOperations = new AtomicInteger(0);
        final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());
        
        try {
            // Create multiple threads that will all try to initialize and use the filter simultaneously
            for (int i = 0; i < threadCount; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        // Wait for all threads to be ready
                        startLatch.await();
                        
                        // Each thread adds multiple entries
                        for (int j = 0; j < entriesPerThread; j++) {
                            String key = "thread-" + threadId + "-key-" + j;
                            lazyFilter.put(key);
                            putOperations.incrementAndGet();
                            
                            // Add a small random delay to increase chance of race condition
                            if (j % 10 == 0) {
                                Thread.sleep(1);
                            }
                        }
                    } catch (Exception e) {
                        exceptions.add(e);
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }
            
            // Start all threads simultaneously
            startLatch.countDown();
            
            // Wait for all threads to complete
            assertTrue("Test timed out", doneLatch.await(30, TimeUnit.SECONDS));
            
            // Verify no exceptions occurred
            if (!exceptions.isEmpty()) {
                fail("Exceptions occurred during concurrent access: " + exceptions.get(0));
            }
            
            // Verify all put operations completed
            assertEquals(threadCount * entriesPerThread, putOperations.get());
            
            // Verify the filter works correctly after concurrent initialization
            for (int i = 0; i < threadCount; i++) {
                for (int j = 0; j < entriesPerThread; j++) {
                    String key = "thread-" + i + "-key-" + j;
                    assertTrue("Filter should contain key: " + key, lazyFilter.mightContain(key));
                }
            }
            
            // Verify false positive behavior (some keys that weren't added should return false)
            int falsePositives = 0;
            int testKeys = 100;
            for (int i = 0; i < testKeys; i++) {
                String nonExistentKey = "non-existent-key-" + i;
                if (lazyFilter.mightContain(nonExistentKey)) {
                    falsePositives++;
                }
            }
            
            // With 1000 entries and 1% FPP, we expect roughly 1% false positives for non-existent keys
            // Allow for some variance but it shouldn't be too high
            assertTrue("False positive rate too high: " + falsePositives + "/" + testKeys, 
                       falsePositives < testKeys * 0.05); // Allow up to 5% to account for variance
            
        } finally {
            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
    }
    
    /**
     * Test concurrent put and mightContain operations to ensure thread safety.
     */
    @Test
    public void testLazyBloomFilterConcurrentReadWrite() throws InterruptedException {
        final int threadCount = 10;
        final int operationsPerThread = 100;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch doneLatch = new CountDownLatch(threadCount);
        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        
        final CacheChangesTracker.LazyBloomFilter lazyFilter = 
            new CacheChangesTracker.LazyBloomFilter(2000);
        
        final AtomicInteger readOperations = new AtomicInteger(0);
        final AtomicInteger writeOperations = new AtomicInteger(0);
        final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());
        
        try {
            // Create mixed read/write threads
            for (int i = 0; i < threadCount; i++) {
                final int threadId = i;
                final boolean isWriter = (i % 2 == 0);
                
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        
                        for (int j = 0; j < operationsPerThread; j++) {
                            String key = "mixed-thread-" + threadId + "-key-" + j;
                            
                            if (isWriter || j < 10) { // Writers, or first few operations of readers
                                lazyFilter.put(key);
                                writeOperations.incrementAndGet();
                            }
                            
                            // All threads also do reads
                            boolean result = lazyFilter.mightContain(key);
                            readOperations.incrementAndGet();
                            
                            // If we just wrote the key, it should definitely be found
                            if (isWriter || j < 10) {
                                assertTrue("Key should be found after being added: " + key, result);
                            }
                        }
                    } catch (Exception e) {
                        exceptions.add(e);
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }
            
            startLatch.countDown();
            assertTrue("Test timed out", doneLatch.await(30, TimeUnit.SECONDS));
            
            if (!exceptions.isEmpty()) {
                fail("Exceptions occurred during concurrent read/write: " + exceptions.get(0));
            }
            
            assertTrue("Should have performed read operations", readOperations.get() > 0);
            assertTrue("Should have performed write operations", writeOperations.get() > 0);
            
        } finally {
            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
    }
    
    /**
     * Test that LazyBloomFilter behaves correctly when filter is never initialized
     * (i.e., only mightContain is called, never put).
     */
    @Test
    public void testLazyBloomFilterNoInitialization() {
        CacheChangesTracker.LazyBloomFilter lazyFilter = 
            new CacheChangesTracker.LazyBloomFilter(1000);
        
        // Should return false for any key when filter is not initialized
        assertFalse(lazyFilter.mightContain("any-key"));
        assertFalse(lazyFilter.mightContain("another-key"));
        assertFalse(lazyFilter.mightContain(""));
    }
} 