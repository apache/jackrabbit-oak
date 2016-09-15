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
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.CheckForNull;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class NRTIndexFactory implements Closeable{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ListMultimap<String, NRTIndex> indexes = LinkedListMultimap.create();
    private final IndexCopier indexCopier;
    private final Clock clock;
    private final long refreshDeltaInSecs;

    public NRTIndexFactory(IndexCopier indexCopier) {
        this(indexCopier, Clock.SIMPLE, 1);
    }

    public NRTIndexFactory(IndexCopier indexCopier, Clock clock, long refreshDeltaInSecs) {
        this.indexCopier = checkNotNull(indexCopier);
        this.clock = clock;
        this.refreshDeltaInSecs = refreshDeltaInSecs;
    }

    //This would not be invoked concurrently
    // but still mark it synchronized for safety
    @CheckForNull
    public synchronized NRTIndex createIndex(IndexDefinition definition) {
        if (!(definition.isNRTIndexingEnabled() || definition.isSyncIndexingEnabled())){
            return null;
        }
        String indexPath = definition.getIndexPathFromConfig();
        NRTIndex current = new NRTIndex(definition, indexCopier, getRefreshPolicy(definition), getPrevious(indexPath));
        indexes.put(indexPath, current);
        closeLast(indexPath);
        return current;
    }

    @Override
    public void close() throws IOException {
        for (NRTIndex index : indexes.values()){
            index.close();
        }
        indexes.clear();
    }

    List<NRTIndex> getIndexes(String path){
        return indexes.get(path);
    }

    private void closeLast(String indexPath) {
        List<NRTIndex> existing = indexes.get(indexPath);
        if (existing.size() < 3){
            return;
        }
        NRTIndex oldest = existing.remove(0);
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
        checkArgument(existing.size() <= 2, "Found [%s] more than 3 index", existing.size());
        return existing.get(existing.size() - 1);
    }

    private ReaderRefreshPolicy getRefreshPolicy(IndexDefinition definition) {
        return new TimedRefreshPolicy(clock, TimeUnit.SECONDS, refreshDeltaInSecs);
    }
}
