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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.guava.common.collect.Lists;

import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException.Type;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

import java.util.LinkedList;

/**
 * Wraps a document store and can be instructed to fail operations.
 */
class FailingDocumentStore extends DocumentStoreWrapper {

    private final Random random;

    private volatile double p;

    private AtomicLong failAfter = new AtomicLong(Long.MAX_VALUE);

    private AtomicLong numFailures = new AtomicLong(0);

    private Type exceptionType = Type.GENERIC;

    private List<Collection<? extends Document>> collectionIncludeList;

    private List<String> idIncludeList;

    private Thread inThread;

    private List<FailedUpdateOpListener> listeners = new ArrayList<>();

    private boolean afterOp = false;

    private boolean noDispose = false;

    class Fail {

        private Fail() {
            never();
        }

        Fail afterOp() {
            afterOp = true;
            return this;
        }

        Fail beforeOp() {
            afterOp = false;
            return this;
        }

        Fail after(int numOps) {
            p = -1;
            failAfter.set(numOps);
            return this;
        }

        Fail withType(Type type) {
            exceptionType = type;
            return this;
        }

        void never() {
            p = -1;
            numFailures.set(0);
            failAfter.set(Long.MAX_VALUE);
            exceptionType = Type.GENERIC;
            collectionIncludeList = null;
            idIncludeList = null;
            afterOp = false;
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

        Fail on(Collection<? extends Document> collectionInclude) {
            if (collectionIncludeList == null) {
                collectionIncludeList = new LinkedList<>();
            }
            collectionIncludeList.add(collectionInclude);
            return this;
        }

        Fail on(String idInclude) {
            if (idIncludeList == null) {
                idIncludeList = new LinkedList<>();
            }
            idIncludeList.add(idInclude);
            return this;
        }

        Fail in(Thread thread) {
            inThread = thread;
            return this;
        }
    }

    public interface FailedUpdateOpListener {

        void failed(UpdateOp op);
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

    void addListener(FailedUpdateOpListener listener) {
        listeners.add(listener);
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection,
                                            String key) {
        if (!afterOp) {
            maybeFail(collection);
        }
        try {
            super.remove(collection, key);
        } finally {
            if (afterOp) {
                maybeFail(collection);
            }
        }
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
            if (!afterOp) {
                maybeFail(collection);
            }
            try {
                num += super.remove(collection, singletonMap(rm.getKey(), rm.getValue()));
            } finally {
                if (afterOp) {
                    maybeFail(collection);
                }
            }
        }
        return num;
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection,
                                           String indexedProperty,
                                           long startValue,
                                           long endValue)
            throws DocumentStoreException {
        if (!afterOp) {
            maybeFail(collection);
        }
        try {
            return super.remove(collection, indexedProperty, startValue, endValue);
        } finally {
            if (afterOp) {
                maybeFail(collection);
            }

        }
    }

    @Override
    public <T extends Document> boolean create(Collection<T> collection,
                                               List<UpdateOp> updateOps) {
        List<UpdateOp> remaining = new ArrayList<>(updateOps);
        int i = 0;
        // create individually
        for (UpdateOp op : updateOps) {
            if (!afterOp) {
                maybeFail(collection, remaining.subList(i++, remaining.size()));
            }
            try {
                if (!super.create(collection, singletonList(op))) {
                    return false;
                }
            } finally {
                if (afterOp) {
                    maybeFail(collection, remaining.subList(i++, remaining.size()));
                }
            }
        }
        return true;
    }

    @Override
    public <T extends Document> T createOrUpdate(Collection<T> collection,
            UpdateOp update) {
        if (!afterOp) {
            maybeFail(collection, singletonList(update));
        }
        try {
            return super.createOrUpdate(collection, update);
        } finally {
            if (afterOp) {
                maybeFail(collection, singletonList(update));
            }
        }
    }

    @Override
    public <T extends Document> List<T> createOrUpdate(Collection<T> collection,
                                                       List<UpdateOp> updateOps) {
        List<UpdateOp> remaining = new ArrayList<>(updateOps);
        List<T> result = new ArrayList<>();
        int i = 0;
        for (UpdateOp op : updateOps) {
            if (!afterOp) {
                maybeFail(collection, remaining.subList(i++, remaining.size()));
            }
            try {
                result.add(super.createOrUpdate(collection, op));
            } finally {
                if (afterOp) {
                    maybeFail(collection, remaining.subList(i++, remaining.size()));
                }
            }
        }
        return result;
    }

    @Override
    public <T extends Document> T findAndUpdate(Collection<T> collection,
                                                UpdateOp update) {
        if (!afterOp) {
            maybeFail(collection, singletonList(update));
        }
        try {
            return super.findAndUpdate(collection, update);
        } finally {
            if (afterOp) {
                maybeFail(collection, singletonList(update));
            }
        }
    }

    private <T extends Document> void maybeFail(Collection<T> collection) {
        maybeFail(collection, Collections.emptyList());
    }

    private <T extends Document> void maybeFail(Collection<T> collection,
                                                List<UpdateOp> remainingOps) {
        if ((collectionIncludeList == null || collectionIncludeList.contains(collection)) &&
                (inThread == null || inThread == Thread.currentThread()) &&
                (random.nextFloat() < p || failAfter.getAndDecrement() <= 0) &&
                (idIncludeList == null || (!remainingOps.isEmpty()
                        && idIncludeList.contains(remainingOps.get(0).getId())))) {
            if (numFailures.getAndDecrement() > 0) {
                reportRemainingOps(remainingOps);
                failNow(remainingOps);
            }
        }
    }

    void failNow(List<UpdateOp> remainingOps) {
        throw new DocumentStoreException("write operation failed", null, exceptionType);
    }

    private void reportRemainingOps(List<UpdateOp> remainingOps) {
        listeners.forEach(listener -> remainingOps.forEach(listener::failed));
    }

    public void noDispose() {
        noDispose = true;
    }

    @Override
    public void dispose() {
        if (!noDispose) {
            super.dispose();
        }
    }

}
