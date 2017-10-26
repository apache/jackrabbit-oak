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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.CheckForNull;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class NRTIndexFactory implements Closeable{
    /**
     * Maximum numbers of NRTIndex to keep at a time. At runtime for a given index
     * /oak:index/fooIndex at max 2 IndexNode would be opened at a time and those 2
     * IndexNode would keep reference to at max 3 NRT Indexes
     */
    private static final int MAX_INDEX_COUNT = 3;
    private static final int REFRESH_DELTA_IN_SECS = Integer.getInteger("oak.lucene.refreshDeltaSecs", 1);
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ListMultimap<String, NRTIndex> indexes = LinkedListMultimap.create();
    private final IndexCopier indexCopier;
    private final Clock clock;
    private final long refreshDeltaInSecs;
    private final StatisticsProvider statisticsProvider;
    private NRTDirectoryFactory directoryFactory = DefaultNRTDirFactory.INSTANCE;
    private boolean assertAllResourcesClosed = Boolean.getBoolean("oak.lucene.assertAllResourcesClosed");

    public NRTIndexFactory(IndexCopier indexCopier, StatisticsProvider statisticsProvider) {
        this(indexCopier, Clock.SIMPLE, REFRESH_DELTA_IN_SECS, statisticsProvider);
    }

    public NRTIndexFactory(IndexCopier indexCopier, Clock clock, long refreshDeltaInSecs,
                           StatisticsProvider statisticsProvider) {
        this.indexCopier = checkNotNull(indexCopier);
        this.clock = clock;
        this.refreshDeltaInSecs = refreshDeltaInSecs;
        this.statisticsProvider = statisticsProvider;
        log.info("Refresh delta set to {} secs", refreshDeltaInSecs);
    }

    //This would not be invoked concurrently
    // but still mark it synchronized for safety
    @CheckForNull
    public synchronized NRTIndex createIndex(IndexDefinition definition) {
        if (!(definition.isNRTIndexingEnabled() || definition.isSyncIndexingEnabled())){
            return null;
        }
        String indexPath = definition.getIndexPath();
        NRTIndex current = new NRTIndex(definition, indexCopier, getRefreshPolicy(definition),
                getPrevious(indexPath), statisticsProvider, directoryFactory, assertAllResourcesClosed);
        indexes.put(indexPath, current);
        closeLast(indexPath);
        return current;
    }

    @Override
    public void close() throws IOException {
        for (String indexPath : indexes.keySet()) {
            //Close backwards i.e. newest NRTIndex first and then older
            //as newer refers to previous NRTIndex readers
            List<NRTIndex> nrtIndexes = indexes.get(indexPath);
            for (int i = nrtIndexes.size() -1 ; i >= 0 ; i--) {
                nrtIndexes.get(i).close();
            }
        }
        indexes.clear();
    }

    List<NRTIndex> getIndexes(String path){
        return indexes.get(path);
    }

    public void setDirectoryFactory(NRTDirectoryFactory directoryFactory) {
        this.directoryFactory = directoryFactory;
    }

    /**
     * Test mode upon which enables assertions to confirm that all readers are closed
     * by the time NRTIndex is closed
     */
    public void setAssertAllResourcesClosed(boolean assertAllResourcesClosed) {
        this.assertAllResourcesClosed = assertAllResourcesClosed;
    }

    private void closeLast(String indexPath) {
        List<NRTIndex> existing = indexes.get(indexPath);
        if (existing.size() <= MAX_INDEX_COUNT){
            return;
        }
        NRTIndex oldest = existing.remove(0);

        //Disconnect the 'oldest' from NRTIndex which refers to that
        //i.e. the next entry in existing
        existing.get(0).disconnectPrevious();
        try {
            oldest.close();
        } catch (IOException e) {
            log.warn("Error occurred while closing index [{}]", oldest, e);
        }
    }

    private NRTIndex getPrevious(String indexPath) {
        List<NRTIndex> existing = indexes.get(indexPath);
        if (existing.isEmpty()){
            return null;
        }
        checkArgument(existing.size() <= MAX_INDEX_COUNT, "Found [%s] more than 3 index", existing.size());
        return existing.get(existing.size() - 1);
    }

    private IndexUpdateListener getRefreshPolicy(IndexDefinition definition) {
        if (definition.isSyncIndexingEnabled()){
            return new RefreshOnWritePolicy();
            //return new RefreshOnReadPolicy(clock, TimeUnit.SECONDS, refreshDeltaInSecs);
        }
        return new TimedRefreshPolicy(clock, TimeUnit.SECONDS, refreshDeltaInSecs);
    }

    private enum DefaultNRTDirFactory implements NRTDirectoryFactory {
        INSTANCE;

        @Override
        public Directory createNRTDir(IndexDefinition definition, File indexDir) throws IOException {
            Directory fsdir = FSDirectory.open(indexDir);
            //TODO make these configurable
            return new NRTCachingDirectory(fsdir, 1, 1);
        }
    }
}
