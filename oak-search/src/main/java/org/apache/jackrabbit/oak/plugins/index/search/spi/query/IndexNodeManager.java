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
package org.apache.jackrabbit.oak.plugins.index.search.spi.query;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.update.ReaderRefreshPolicy;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.getAsyncLaneName;

/**
 * Keeps track of the open read sessions for an index.
 */
public abstract class IndexNodeManager {
    /**
     * Name of the hidden node under which information about the checkpoints
     * seen and indexed by each async indexer is kept.
     */
    public static final String ASYNC = ":async";

    private static final AtomicInteger SEARCHER_ID_COUNTER = new AtomicInteger();

    private static final PerfLogger PERF_LOGGER =
            new PerfLogger(LoggerFactory.getLogger(IndexNodeManager.class.getName() + ".perf"));

    protected abstract IndexNodeManager open(String indexPath, NodeState root, NodeState defnNodeState);

    protected abstract void releaseResources();

    protected abstract IndexNode getIndexNode();

    protected abstract ReaderRefreshPolicy getReaderRefreshPolicy();

    protected abstract void refreshReaders();

    protected abstract String getName();

    protected abstract IndexDefinition getDefinition();

    static boolean hasAsyncIndexerRun(NodeState root, String indexPath, NodeState defnNodeState) {
        boolean hasAsyncNode = root.hasChildNode(ASYNC);

        String asyncLaneName = getAsyncLaneName(defnNodeState, indexPath, defnNodeState.getProperty(ASYNC_PROPERTY_NAME));

        if (asyncLaneName != null) {
            return hasAsyncNode && root.getChildNode(ASYNC).hasProperty(asyncLaneName);
        } else {
            // useful only for tests - basically non-async index defs which don't rely on /:async
            // hence either readers are there (and this method doesn't come into play during open)
            // OR there is no cycle (where we return false correctly)
            return  false;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(IndexNodeManager.class);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Semaphore refreshLock = new Semaphore(1);

    private final Runnable refreshCallback = new Runnable() {
        @Override
        public void run() {
            if (refreshLock.tryAcquire()) {
                try {
                    refreshReaders();
                }finally {
                    refreshLock.release();
                }
            }
        }
    };

    private boolean closed = false;


    @Nullable
    IndexNode acquire() {
        lock.readLock().lock();
        if (closed) {
            lock.readLock().unlock();
            return null;
        } else {
            boolean success = false;
            try {
                getReaderRefreshPolicy().refreshOnReadIfRequired(refreshCallback);
                IndexNode indexNode = getIndexNode();
                success = true;
                return indexNode;
            } finally {
                if (!success) {
                    lock.readLock().unlock();
                }
            }
        }
    }

    private void release() {
        lock.readLock().unlock();
    }

    void close() throws IOException {
        lock.writeLock().lock();
        try {
            checkState(!closed);
            closed = true;
        } finally {
            lock.writeLock().unlock();
        }

        releaseResources();
    }

}
