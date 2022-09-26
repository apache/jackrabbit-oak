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
package org.apache.jackrabbit.oak.plugins.document.check;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.jackrabbit.oak.spi.state.NodeStateUtils.getNode;

/**
 * A {@link DocumentProcessor} that processes {@link NodeState}s.
 */
public abstract class AsyncNodeStateProcessor extends AsyncDocumentProcessor {


    protected final DocumentNodeStore ns;

    protected final RevisionVector headRevision;

    protected final NodeState uuidIndex;

    protected AsyncNodeStateProcessor(DocumentNodeStore ns,
                                      RevisionVector headRevision,
                                      ExecutorService executorService) {
        super(executorService);
        this.ns = ns;
        this.headRevision = headRevision;
        this.uuidIndex = getNode(ns.getRoot(), "/oak:index/uuid/:index");
    }

    /**
     * Decide early whether a {@link NodeDocument} should be processed or not.
     * This implementation returns {@code true} if the document not a split
     * document, otherwise {@code false}. This method can be overridden by
     * subclasses.
     *
     * @param doc the document to process.
     * @return whether the document should be processed or ignored.
     */
    protected boolean process(NodeDocument doc) {
        return !doc.isSplitDocument();
    }

    /**
     * Utility method that resolves he {@code uuid} into a {@link NodeState}
     * with the help of the UUID index.
     *
     * @param uuid the UUID to resolve.
     * @param resolvedPath will be set to the resolved path if available.
     * @return the {@link NodeState} with the given UUID or {@code null} if it
     *         cannot be resolved or doesn't exist.
     */
    protected final @Nullable NodeState getNodeByUUID(@NotNull String uuid,
                                                      @NotNull AtomicReference<String> resolvedPath) {
        PropertyState entry = uuidIndex.getChildNode(uuid).getProperty("entry");
        NodeState state = null;
        if (entry != null && entry.isArray() && entry.count() > 0) {
            String path = entry.getValue(Type.STRING, 0);
            resolvedPath.set(path);
            state = getNode(ns.getRoot(), path);
            if (!state.exists()) {
                state = null;
            }
        }
        return state;
    }

    @Override
    protected final Optional<Callable<Void>> createTask(@NotNull NodeDocument document,
                                                        @NotNull BlockingQueue<Result> results) {
        if (process(document)) {
            return Optional.of(new NodeStateTask(document, results));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Responsibility of the subclass to implement the processor logic. This
     * method will run as a task with an executor service.
     *
     * @param path the path of the {@code NodeState} to process.
     * @param state the {@code NodeState} or {@code null} if the node does not
     *          exist at this path. This may happen for nodes that have been
     *          deleted but not yet garbage collected.
     * @return optional result of the task.
     */
    protected abstract Optional<Result> runTask(@NotNull Path path,
                                                @Nullable NodeState state);

    protected class NodeStateTask implements Callable<Void> {

        private final NodeDocument document;

        private final BlockingQueue<Result> results;

        public NodeStateTask(@NotNull NodeDocument document,
                             @NotNull BlockingQueue<Result> results) {
            this.document = document;
            this.results = results;
        }

        @Override
        public Void call() throws Exception {
            Path path = document.getPath();
            NodeState state = document.getNodeAtRevision(ns, headRevision, null);
            runTask(path, state).ifPresent(this::collect);
            return null;
        }

        private void collect(Result r) {
            try {
                results.put(r);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while collecting result", e);
            }
        }
    }
}
