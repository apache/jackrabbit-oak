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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Lists;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

/**
 * Wraps a document store and can be instructed to fail operations.
 */
class FailingDocumentStore extends DocumentStoreWrapper {

    private final Random random;

    private volatile double p;

    private AtomicLong failAfter = new AtomicLong(Long.MAX_VALUE);

    private AtomicLong numFailures = new AtomicLong(0);

    class Fail {

        private Fail() {
            never();
        }

        Fail after(int numOps) {
            p = -1;
            failAfter.set(numOps);
            return this;
        }

        void never() {
            p = -1;
            numFailures.set(0);
            failAfter.set(Long.MAX_VALUE);
        }

        void once() {
            numFailures.set(1);
        }

        void eternally() {
            numFailures.set(Long.MAX_VALUE);
        }

        Fail randomly(double probability) {
            p = probability;
            return this;
        }
    }

    FailingDocumentStore(DocumentStore store, long seed) {
        this(store, new Random(seed));
    }

    FailingDocumentStore(DocumentStore store) {
        this(store, new Random());
    }

    private FailingDocumentStore(DocumentStore store, Random r) {
        super(store);
        this.random = r;
    }

    Fail fail() {
        return new Fail();
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection,
                                            String key) {
        maybeFail();
        super.remove(collection, key);
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection,
                                            List<String> keys) {
        // redirect to single document remove method
        for (String k : keys) {
            remove(collection, k);
        }
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection,
                                           Map<String, Long> toRemove) {
        int num = 0;
        // remove individually
        for (Map.Entry<String, Long> rm : toRemove.entrySet()) {
            maybeFail();
            num += super.remove(collection, singletonMap(rm.getKey(), rm.getValue()));
        }
        return num;
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection,
                                           String indexedProperty,
                                           long startValue,
                                           long endValue)
            throws DocumentStoreException {
        maybeFail();
        return super.remove(collection, indexedProperty, startValue, endValue);
    }

    @Override
    public <T extends Document> boolean create(Collection<T> collection,
                                               List<UpdateOp> updateOps) {
        // create individually
        for (UpdateOp op : updateOps) {
            maybeFail();
            if (!super.create(collection, singletonList(op))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public <T extends Document> T createOrUpdate(Collection<T> collection,
                                                 UpdateOp update) {
        maybeFail();
        return super.createOrUpdate(collection, update);
    }

    @Override
    public <T extends Document> List<T> createOrUpdate(Collection<T> collection,
                                                       List<UpdateOp> updateOps) {
        List<T> result = Lists.newArrayList();
        // redirect to single document createOrUpdate
        for (UpdateOp op : updateOps) {
            result.add(createOrUpdate(collection, op));
        }
        return result;
    }

    @Override
    public <T extends Document> T findAndUpdate(Collection<T> collection,
                                                UpdateOp update) {
        maybeFail();
        return super.findAndUpdate(collection, update);
    }

    private void maybeFail() {
        if (random.nextFloat() < p || failAfter.getAndDecrement() <= 0) {
            if (numFailures.getAndDecrement() > 0) {
                throw new DocumentStoreException("write operation failed");
            }
        }
    }
}
