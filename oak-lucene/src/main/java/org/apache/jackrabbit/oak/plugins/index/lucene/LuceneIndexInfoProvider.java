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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfo;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexInfo;
import org.apache.jackrabbit.oak.plugins.index.IndexInfoProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.DirectoryUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexConsistencyChecker;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.MultiplexersLucene;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class LuceneIndexInfoProvider implements IndexInfoProvider {

    private final NodeStore nodeStore;

    private final AsyncIndexInfoService asyncInfoService;

    private final File workDir;

    public LuceneIndexInfoProvider(NodeStore nodeStore, AsyncIndexInfoService asyncInfoService, File workDir) {
        this.nodeStore = checkNotNull(nodeStore);
        this.asyncInfoService = checkNotNull(asyncInfoService);
        this.workDir = checkNotNull(workDir);
    }

    static String getAsyncName(NodeState idxState, String indexPath) {
        PropertyState async = idxState.getProperty(IndexConstants.ASYNC_PROPERTY_NAME);
        if (async != null) {
            Set<String> asyncNames = Sets.newHashSet(async.getValue(Type.STRINGS));
            asyncNames.remove(IndexConstants.INDEXING_MODE_NRT);
            asyncNames.remove(IndexConstants.INDEXING_MODE_SYNC);
            checkArgument(!asyncNames.isEmpty(), "No valid async name found for " +
                    "index [%s], definition %s", indexPath, idxState);
            return Iterables.getOnlyElement(asyncNames);
        }
        return null;
    }

    @Override
    public String getType() {
        return LuceneIndexConstants.TYPE_LUCENE;
    }

    @Override
    public IndexInfo getInfo(String indexPath) throws IOException {
        NodeState idxState = NodeStateUtils.getNode(nodeStore.getRoot(), indexPath);

        checkArgument(LuceneIndexConstants.TYPE_LUCENE.equals(idxState.getString(IndexConstants.TYPE_PROPERTY_NAME)),
                "Index definition at [%s] is not of type 'lucene'", indexPath);

        LuceneIndexInfo info = new LuceneIndexInfo(indexPath);
        computeSize(idxState, info);
        computeAsyncIndexInfo(idxState, indexPath, info);
        return info;
    }

    @Override
    public boolean isValid(String indexPath) throws IOException {
        IndexConsistencyChecker checker = new IndexConsistencyChecker(nodeStore.getRoot(), indexPath, workDir);
        return checker.check(IndexConsistencyChecker.Level.BLOBS_ONLY).clean;
    }

    private void computeAsyncIndexInfo(NodeState idxState, String indexPath, LuceneIndexInfo info) {
        String asyncName = getAsyncName(idxState, indexPath);
        checkNotNull(asyncName, "No 'async' value for index definition " +
                "at [%s]. Definition %s", indexPath, idxState);

        AsyncIndexInfo asyncInfo = asyncInfoService.getInfo(asyncName);
        checkNotNull(asyncInfo, "No async info found for name [%s] " +
                "for index at [%s]", asyncName, indexPath);

        info.indexedUptoTime = asyncInfo.getLastIndexedTo();
        info.asyncName = asyncName;
    }

    private void computeSize(NodeState idxState, LuceneIndexInfo info) throws IOException {
        IndexDefinition defn = IndexDefinition.newBuilder(nodeStore.getRoot(), idxState, info.indexPath).build();
        for (String dirName : idxState.getChildNodeNames()) {
            if (NodeStateUtils.isHidden(dirName) && MultiplexersLucene.isIndexDirName(dirName)) {
                Directory dir = new OakDirectory(new ReadOnlyBuilder(idxState), dirName, defn, true);
                try (DirectoryReader dirReader = DirectoryReader.open(dir)) {
                    info.numEntries += dirReader.numDocs();
                    info.size = DirectoryUtils.dirSize(dir);
                }
            }
        }
    }

    private static class LuceneIndexInfo implements IndexInfo {
        String indexPath;
        String asyncName;
        long numEntries;
        long size;
        long indexedUptoTime;

        public LuceneIndexInfo(String indexPath) {
            this.indexPath = indexPath;
        }

        @Override
        public String getIndexPath() {
            return indexPath;
        }

        @Override
        public String getType() {
            return LuceneIndexConstants.TYPE_LUCENE;
        }

        @Override
        public String getAsyncLaneName() {
            return asyncName;
        }

        @Override
        public long getLastUpdatedTime() {
            return 0; //TODO To be computed
        }

        @Override
        public long getIndexedUpToTime() {
            return indexedUptoTime;
        }

        @Override
        public long getEstimatedEntryCount() {
            return numEntries;
        }

        @Override
        public long getSizeInBytes() {
            return size;
        }

        @Override
        public boolean hasIndexDefinitionChangedWithoutReindexing() {
            return false; //TODO To be computed
        }
    }
}
