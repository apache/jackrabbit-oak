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
package org.apache.jackrabbit.oak.plugins.index;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.getBoolean;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.getString;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.isIndexNodeType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncIndexUpdate implements Runnable {

    private static final Logger log = LoggerFactory
            .getLogger(AsyncIndexUpdate.class);

    private final static int CONFIG_WATCH_DELAY_MS = 30000;

    // TODO index impl run frequency could be picked up from the index config
    // directly
    private final static int INDEX_TASK_DELAY_MS = 5000;

    private final NodeStore store;

    private final ScheduledExecutorService executor;

    private final IndexEditorProvider provider;

    private NodeState current = EmptyNodeState.EMPTY_NODE;

    final Map<String, IndexTask> active = new ConcurrentHashMap<String, IndexTask>();

    private boolean started;

    public AsyncIndexUpdate(@Nonnull NodeStore store,
            @Nonnull ScheduledExecutorService executor,
            @Nonnull IndexEditorProvider provider) {
        this.store = checkNotNull(store);
        this.executor = checkNotNull(executor);
        this.provider = checkNotNull(provider);
    }

    public synchronized void start() {
        if (started) {
            log.error("Background index config watcher task already started");
            return;
        }
        started = true;
        executor.scheduleWithFixedDelay(this, 100, CONFIG_WATCH_DELAY_MS,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        log.debug("Running background index config watcher task");
        NodeState after = store.getRoot();
        try {
            EditorHook hook = new EditorHook(new EditorProvider() {
                @Override
                public Editor getRootEditor(NodeState before, NodeState after,
                        NodeBuilder builder) {
                    return VisibleEditor.wrap(new IndexConfigWatcher());
                }
            });
            hook.processCommit(current, after);
            current = after;
        } catch (CommitFailedException e) {
            log.warn("IndexTask update failed", e);
        }
    }

    public synchronized void replace(Map<String, Set<String>> async) {
        Set<String> in = new HashSet<String>(async.keySet());
        Set<String> existing = active.keySet();
        for (String type : existing) {
            if (in.remove(type)) {
                Set<String> defs = async.get(type);
                if (defs.isEmpty()) {
                    remove(type);
                } else {
                    addOrUpdate(type, defs);
                }
            } else {
                remove(type);
            }
        }
        for (String type : in) {
            addOrUpdate(type, async.get(type));
        }
    }

    void addOrUpdate(String type, Set<String> defs) {
        IndexTask task = active.get(type);
        if (task == null) {
            task = new IndexTask(store, provider, type, defs);
            active.put(type, task);
            task.start(executor);
        } else {
            task.update(defs);
        }
    }

    void remove(String type) {
        IndexTask task = active.remove(type);
        if (task != null) {
            task.stop();
        }
    }

    /**
     * This Editor is responsible for watching over changes on async index defs:
     * added index defs and deleted index defs are pushed forward to the
     * #replace method
     * 
     */
    class IndexConfigWatcher extends DefaultEditor {

        private final Map<String, Set<String>> async = new HashMap<String, Set<String>>();

        @Override
        public void enter(NodeState before, NodeState after)
                throws CommitFailedException {
            if (!after.hasChildNode(INDEX_DEFINITIONS_NAME)) {
                return;
            }
            NodeState index = after.getChildNode(INDEX_DEFINITIONS_NAME);
            for (String indexName : index.getChildNodeNames()) {
                NodeState indexChild = index.getChildNode(indexName);
                if (isIndexNodeType(indexChild)) {
                    boolean isasync = getBoolean(indexChild,
                            ASYNC_PROPERTY_NAME);
                    String type = getString(indexChild, TYPE_PROPERTY_NAME);
                    if (type == null || !isasync) {
                        // skip null and non-async types
                        continue;
                    }
                    Set<String> defs = async.get(type);
                    if (defs == null) {
                        defs = new HashSet<String>();
                        async.put(type, defs);
                    }
                    defs.add(type);
                }
            }
        }

        @Override
        public void leave(NodeState before, NodeState after)
                throws CommitFailedException {
            replace(async);
            async.clear();
        }

    }

    static class IndexTask implements Runnable {

        private static final Logger log = LoggerFactory
                .getLogger(IndexTask.class);

        private final NodeStore store;

        private final String type;
        private Set<String> defs;

        private final IndexEditorProvider provider;

        private ScheduledFuture<?> future;

        private NodeState before;

        public IndexTask(NodeStore store, IndexEditorProvider provider,
                String type, Set<String> defs) {
            this.store = store;
            this.provider = provider;
            this.type = type;
            this.defs = defs;
            this.before = EmptyNodeState.EMPTY_NODE;
        }

        public void update(Set<String> defs) {
            // check of there are any changes
            // TODO what happens when I move a def? (rm + add appears as a no-op
            // in the set)
            if (this.defs.equals(defs)) {
                // no-op
                return;
            }

            log.debug("Updated index def for type {}, reindexing", type);
            this.defs = defs;
            this.before = EmptyNodeState.EMPTY_NODE;
        }

        public synchronized void start(ScheduledExecutorService executor) {
            if (future != null) {
                throw new IllegalStateException("IndexTask has already started");
            }
            future = executor.scheduleWithFixedDelay(this, 100,
                    INDEX_TASK_DELAY_MS, TimeUnit.MILLISECONDS);
        }

        public synchronized void stop() {
            if (future == null) {
                log.warn("IndexTask has already stopped.");
                return;
            }
            future.cancel(true);
        }

        @Override
        public void run() {
            log.debug("Running background index task for type {}.", type);
            NodeStoreBranch branch = store.branch();
            NodeState after = branch.getHead();
            try {
                EditorHook hook = new EditorHook(new TypedEditorProvider(
                        provider, type));
                NodeState processed = hook.processCommit(before, after);
                branch.setRoot(processed);
                branch.merge(EmptyHook.INSTANCE);
                before = after;
            } catch (CommitFailedException e) {
                log.warn("IndexTask update failed", e);
            }
        }
    }

    /**
     * This creates a composite editor from a type-filtered index provider.
     * 
     */
    private static class TypedEditorProvider implements EditorProvider {

        private final IndexEditorProvider provider;

        private final String type;

        public TypedEditorProvider(IndexEditorProvider provider, String type) {
            this.type = type;
            this.provider = provider;
        }

        /**
         * This does not make any effort to filter async definitions. The
         * assumption is that given an index type, all of the returned index
         * hooks inherit the same async assumption.
         * 
         */
        @Override
        public Editor getRootEditor(NodeState before, NodeState after,
                NodeBuilder builder) {
            return VisibleEditor.wrap(provider.getIndexEditor(type, builder));
        }
    }

}
