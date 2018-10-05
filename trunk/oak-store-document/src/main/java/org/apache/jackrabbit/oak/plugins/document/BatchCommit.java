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
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;

/**
 * Combines multiple {@link UpdateOp} into a single call to the
 * {@link DocumentStore}.
 */
final class BatchCommit {

    private static final Logger LOG = LoggerFactory.getLogger(BatchCommit.class);

    private final CountDownLatch finished = new CountDownLatch(1);
    private final String id;
    private final BatchCommitQueue queue;

    private List<UpdateOp> ops;
    private List<Future<NodeDocument>> results;

    private boolean executing;

    BatchCommit(String id, BatchCommitQueue queue, boolean onHold) {
        this.id = id;
        this.queue = queue;
        if (onHold) {
            ops = Lists.newArrayList();
            results = Lists.newArrayList();
        }
    }

    String getId() {
        return id;
    }

    Callable<NodeDocument> enqueue(final UpdateOp op) {
        checkArgument(op.getId().equals(id),
                "Cannot add UpdateOp with id %s to BatchCommit with id %s",
                op.getId(), id);
        Callable<NodeDocument> result;
        synchronized (this) {
            checkState(!executing, "Cannot enqueue when batch is already executing");
            if (ops != null) {
                ops.add(op);
                result = new Callable<NodeDocument>() {
                    int idx = ops.size() - 1;
                    @Override
                    public NodeDocument call() throws Exception {
                        synchronized (BatchCommit.this) {
                            while (!executing) {
                                LOG.debug("Waiting until BatchCommit is executing. {}", id);
                                BatchCommit.this.wait();
                            }
                        }
                        try {
                            return execute(idx).get();
                        } catch (ExecutionException e) {
                            throw DocumentStoreException.convert(e.getCause());
                        }
                    }
                };
            } else {
                // not on hold and no other operation in this batch
                executing = true;
                result = new Callable<NodeDocument>() {
                    @Override
                    public NodeDocument call() throws Exception {
                        try {
                            return queue.getStore().findAndUpdate(NODES, op);
                        } finally {
                            queue.finished(BatchCommit.this);
                        }
                    }
                };
            }
        }
        return result;
    }

    void release() {
        synchronized (this) {
            executing = true;
            notifyAll();
        }
    }

    Future<NodeDocument> execute(int idx) {
        if (idx == 0) {
            NodeDocument before = null;
            try {
                UpdateOp combined = UpdateOp.combine(id, ops);
                LOG.debug("Batch committing {} updates", ops.size());
                before = queue.getStore().findAndUpdate(NODES, combined);
            } catch (Throwable t) {
                LOG.warn("BatchCommit failed, will retry individually. " + t.getMessage());
            } finally {
                queue.finished(this);
            }
            try {
                if (before == null) {
                    // batch commit unsuccessful, execute individually
                    executeIndividually();
                } else {
                    populateResults(before);
                }
            } finally {
                finished.countDown();
            }
        } else {
            try {
                finished.await();
            } catch (InterruptedException e) {
                String msg = "Interrupted while waiting for batch commit to finish";
                return Futures.immediateFailedFuture(new DocumentStoreException(msg));
            }
        }
        return results.get(idx);
    }

    void executeIndividually() {
        DocumentStore store = queue.getStore();
        for (UpdateOp op : ops) {
            SettableFuture<NodeDocument> result = SettableFuture.create();
            try {
                result.set(store.findAndUpdate(NODES, op));
            } catch (Throwable t) {
                result.setException(t);
            }
            results.add(result);
        }
    }

    void populateResults(NodeDocument before) {
        DocumentStore store = queue.getStore();
        for (UpdateOp op : ops) {
            results.add(Futures.immediateFuture(before));
            NodeDocument after = new NodeDocument(store);
            before.deepCopy(after);
            UpdateUtils.applyChanges(after, op);
            before = after;
        }
    }
}
