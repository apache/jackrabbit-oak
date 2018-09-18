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

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.json.JsopDiff;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfo;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexInfo;
import org.apache.jackrabbit.oak.plugins.index.IndexInfoProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.DirectoryUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexConsistencyChecker;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.OakDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.MultiplexersLucene;
import org.apache.jackrabbit.oak.spi.state.EqualsDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.apache.jackrabbit.util.ISO8601;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition.INDEX_DEFINITION_NODE;

public class LuceneIndexInfoProvider implements IndexInfoProvider {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final NodeStore nodeStore;

    private final AsyncIndexInfoService asyncInfoService;

    private final File workDir;

    public LuceneIndexInfoProvider(NodeStore nodeStore, AsyncIndexInfoService asyncInfoService, File workDir) {
        this.nodeStore = checkNotNull(nodeStore);
        this.asyncInfoService = checkNotNull(asyncInfoService);
        this.workDir = checkNotNull(workDir);
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
        computeIndexDefinitionChange(idxState, info);
        computeLastUpdatedTime(idxState, info);
        computeAsyncIndexInfo(idxState, indexPath, info);
        return info;
    }

    @Override
    public boolean isValid(String indexPath) throws IOException {
        IndexConsistencyChecker checker = new IndexConsistencyChecker(nodeStore.getRoot(), indexPath, workDir);

        boolean result = false;
        try{
            result = checker.check(IndexConsistencyChecker.Level.BLOBS_ONLY).clean;
        } catch (Exception e) {
            log.warn("Error occurred while performing consistency check for {}", indexPath, e);
        }
        return result;
    }

    private void computeAsyncIndexInfo(NodeState idxState, String indexPath, LuceneIndexInfo info) {
        String asyncName = IndexUtils.getAsyncLaneName(idxState, indexPath);
        if (asyncName == null) {
            log.warn("No 'async' value for index definition at [{}]. Definition {}", indexPath, idxState);
            return;
        }

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
                try (Directory dir = new OakDirectory(new ReadOnlyBuilder(idxState), dirName, defn, true)) {
                    info.numEntries += DirectoryUtils.getNumDocs(dir);
                    info.size = DirectoryUtils.dirSize(dir);
                }
            }
        }
    }

    private static void computeLastUpdatedTime(NodeState idxState, LuceneIndexInfo info) {
        NodeState status = idxState.getChildNode(IndexDefinition.STATUS_NODE);
        if (status.exists()){
            PropertyState updatedTime = status.getProperty(IndexDefinition.STATUS_LAST_UPDATED);
            if (updatedTime != null) {
                info.lastUpdatedTime = ISO8601.parse(updatedTime.getValue(Type.DATE)).getTimeInMillis();
            }
        }
    }

    private static void computeIndexDefinitionChange(NodeState idxState, LuceneIndexInfo info) {
        NodeState storedDefn = idxState.getChildNode(INDEX_DEFINITION_NODE);
        if (storedDefn.exists()) {
            NodeState currentDefn = NodeStateCloner.cloneVisibleState(idxState);
            if (!FilteringEqualsDiff.equals(storedDefn, currentDefn)){
                info.indexDefinitionChanged = true;
                info.indexDiff = JsopDiff.diffToJsop(storedDefn, currentDefn);
            }
        }
    }

    private static class LuceneIndexInfo implements IndexInfo {
        String indexPath;
        String asyncName;
        long numEntries;
        long size;
        long indexedUptoTime;
        long lastUpdatedTime;
        boolean indexDefinitionChanged;
        String indexDiff;

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
            return lastUpdatedTime;
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
            return indexDefinitionChanged;
        }

        @Override
        public String getIndexDefinitionDiff() {
            return indexDiff;
        }
    }

    static class FilteringEqualsDiff extends EqualsDiff {
        private static final Set<String> IGNORED_PROP_NAMES = ImmutableSet.of(
                IndexConstants.REINDEX_COUNT,
                IndexConstants.REINDEX_PROPERTY_NAME
        );
        public static boolean equals(NodeState before, NodeState after) {
            return before.exists() == after.exists()
                    && after.compareAgainstBaseState(before, new FilteringEqualsDiff());
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            if (ignoredProp(before.getName())){
                return true;
            }
            return false;
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            if (ignoredProp(after.getName())){
                return true;
            }
            return super.propertyAdded(after);
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            if (ignoredProp(before.getName())){
                return true;
            }
            return super.propertyDeleted(before);
        }

        private boolean ignoredProp(String name) {
            return IGNORED_PROP_NAMES.contains(name) || NodeStateUtils.isHidden(name);
        }
    }
}
