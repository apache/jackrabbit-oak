/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.commons.cache;

import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;
import org.apache.jackrabbit.oak.commons.concurrent.Concurrent;
import org.junit.Test;

/**
 * Tests the cache implementation.
 */
public class ConcurrentCacheTest implements Cache.Backend<Integer, ConcurrentCacheTest.Data> {

    Cache<Integer, Data> cache = Cache.newInstance(this, 5);
    AtomicInteger counter = new AtomicInteger();
    volatile int value;

    @Test
    public void test() throws Exception {
        Concurrent.run("cache", new Concurrent.Task() {
            @Override
            public void call() throws Exception {
                int k = value++ % 10;
                Data v = cache.get(k);
                Assert.assertEquals(k, v.value);
            }
        });
    }

    @Override
    public Data load(Integer key) {
        int start = counter.get();
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            // ignore
        }
        if (counter.getAndIncrement() != start) {
            throw new AssertionError("Concurrent load");
        }
        return new Data(key);
    }

    static class Data implements Cache.Value {

        int value;

        Data(int value) {
            this.value = value;
        }

        @Override
        public int getMemory() {
            return 1;
        }

    }
}
