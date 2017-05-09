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

package org.apache.jackrabbit.oak.plugins.index.lucene.hybrid;

import java.util.concurrent.Executor;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.observation.Filter;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ExternalObserverBuilder {
    private static final Logger log = LoggerFactory.getLogger(ExternalIndexObserver.class);
    private final IndexingQueue indexingQueue;
    private final IndexTracker indexTracker;
    private final StatisticsProvider statisticsProvider;
    private final Executor executor;
    private final int queueSize;
    private BackgroundObserver backgroundObserver;
    private FilteringObserver filteringObserver;

    public ExternalObserverBuilder(IndexingQueue indexingQueue, IndexTracker indexTracker,
                                   StatisticsProvider statisticsProvider,
                                   Executor executor, int queueSize) {
        this.indexingQueue = checkNotNull(indexingQueue);
        this.indexTracker = checkNotNull(indexTracker);
        this.statisticsProvider = checkNotNull(statisticsProvider);
        this.executor = checkNotNull(executor);
        this.queueSize = queueSize;
    }

    public Observer build() {
        if (filteringObserver != null) {
            return filteringObserver;
        }
        ExternalIndexObserver externalObserver = new ExternalIndexObserver(indexingQueue, indexTracker, statisticsProvider);
        backgroundObserver = new WarningObserver(externalObserver, executor, queueSize);
        filteringObserver = new FilteringObserver(backgroundObserver, externalObserver);
        return filteringObserver;
    }

    public BackgroundObserver getBackgroundObserver() {
        return backgroundObserver;
    }

    private static class WarningObserver extends BackgroundObserver {
        private final int queueLength;

        public WarningObserver(@Nonnull Observer observer, @Nonnull Executor executor, int queueLength) {
            super(observer, executor, queueLength);
            this.queueLength = queueLength;
        }

        @Override
        protected void added(int queueSize) {
            //TODO Have a variant of BackgroundObserver which drops elements from the tail
            //as for indexing case its fine to drop older stuff
            if (queueSize >= queueLength) {
                log.warn("External observer queue is full");
            }
        }
    }

    private static class FilteringObserver implements Observer {
        private final Observer delegate;
        private final Filter filter;

        private FilteringObserver(Observer delegate, Filter filter) {
            this.delegate = delegate;
            this.filter = filter;
        }

        @Override
        public void contentChanged(@Nonnull NodeState root, @Nonnull CommitInfo info) {
            //TODO Optimize for the case where new async index update is detected. Then
            //existing items in queue should not be processed

            //We need to only pass on included changes. Not using FilteringAwareObserver
            //As here the filtering logic only relies on CommitContext and not concerned
            //with before state
            if (!filter.excludes(root, info)) {
                delegate.contentChanged(root, info);
            }
        }
    }

}
