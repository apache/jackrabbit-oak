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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.IOException;
import java.util.Calendar;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexingContext;
import org.apache.jackrabbit.oak.plugins.index.lucene.binary.BinaryTextExtractor;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.FacetHelper;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.FacetsConfigProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriterFactory;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.util.ISO8601;
import org.apache.lucene.facet.FacetsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition.INDEX_DEFINITION_NODE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROP_REFRESH_DEFN;

public class LuceneIndexEditorContext implements FacetsConfigProvider{

    private static final Logger log = LoggerFactory
            .getLogger(LuceneIndexEditorContext.class);

    private static final PerfLogger PERF_LOGGER =
            new PerfLogger(LoggerFactory.getLogger(LuceneIndexEditorContext.class.getName() + ".perf"));

    private FacetsConfig facetsConfig;

    private IndexDefinition definition;

    private final NodeBuilder definitionBuilder;

    private final LuceneIndexWriterFactory indexWriterFactory;

    private LuceneIndexWriter writer = null;

    private long indexedNodes;

    private final IndexUpdateCallback updateCallback;

    private boolean reindex;

    private final ExtractedTextCache extractedTextCache;

    private final IndexAugmentorFactory augmentorFactory;

    private final NodeState root;

    private final IndexingContext indexingContext;

    private final boolean asyncIndexing;

    //Intentionally static, so that it can be set without passing around clock objects
    //Set for testing ONLY
    private static Clock clock = Clock.SIMPLE;

    private final boolean indexDefnRewritten;

    private BinaryTextExtractor textExtractor;

    private PropertyUpdateCallback propertyUpdateCallback;

    LuceneIndexEditorContext(NodeState root, NodeBuilder definition,
                             @Nullable IndexDefinition indexDefinition,
                             IndexUpdateCallback updateCallback,
                             LuceneIndexWriterFactory indexWriterFactory,
                             ExtractedTextCache extractedTextCache,
                             IndexAugmentorFactory augmentorFactory,
                             IndexingContext indexingContext, boolean asyncIndexing) {
        this.root = root;
        this.indexingContext = checkNotNull(indexingContext);
        this.definitionBuilder = definition;
        this.indexWriterFactory = indexWriterFactory;
        this.definition = indexDefinition != null ? indexDefinition :
                createIndexDefinition(root, definition, indexingContext, asyncIndexing);
        this.indexedNodes = 0;
        this.updateCallback = updateCallback;
        this.extractedTextCache = extractedTextCache;
        this.augmentorFactory = augmentorFactory;
        this.asyncIndexing = asyncIndexing;
        if (this.definition.isOfOldFormat()){
            indexDefnRewritten = true;
            IndexDefinition.updateDefinition(definition, indexingContext.getIndexPath());
        } else {
            indexDefnRewritten = false;
        }
    }

    LuceneIndexWriter getWriter() throws IOException {
        if (writer == null) {
            //Lazy initialization so as to ensure that definition is based
            //on latest NodeBuilder state specially in case of reindexing
            writer = indexWriterFactory.newInstance(definition, definitionBuilder, reindex);
        }
        return writer;
    }

    public IndexingContext getIndexingContext() {
        return indexingContext;
    }

    @CheckForNull
    public PropertyUpdateCallback getPropertyUpdateCallback() {
        return propertyUpdateCallback;
    }

    void setPropertyUpdateCallback(PropertyUpdateCallback propertyUpdateCallback) {
        this.propertyUpdateCallback = propertyUpdateCallback;
    }

    /**
     * close writer if it's not null
     */
    void closeWriter() throws IOException {
        Calendar currentTime = getCalendar();
        final long start = PERF_LOGGER.start();
        boolean indexUpdated = getWriter().close(currentTime.getTimeInMillis());

        if (indexUpdated) {
            PERF_LOGGER.end(start, -1, "Closed writer for directory {}", definition);
            //OAK-2029 Record the last updated status so
            //as to make IndexTracker detect changes when index
            //is stored in file system
            NodeBuilder status = definitionBuilder.child(IndexDefinition.STATUS_NODE);
            status.setProperty(IndexDefinition.STATUS_LAST_UPDATED, getUpdatedTime(currentTime), Type.DATE);
            status.setProperty("indexedNodes", indexedNodes);

            PERF_LOGGER.end(start, -1, "Overall Closed IndexWriter for directory {}", definition);

            if (textExtractor != null){
                textExtractor.done(reindex);
            }
        }
    }

    private String getUpdatedTime(Calendar currentTime) {
        CommitInfo info = getIndexingContext().getCommitInfo();
        String checkpointTime = (String) info.getInfo().get(IndexConstants.CHECKPOINT_CREATION_TIME);
        if (checkpointTime != null) {
            return checkpointTime;
        }
        return ISO8601.format(currentTime);
    }

    /** Only set for testing */
    static void setClock(Clock c) {
        checkNotNull(c);
        clock = c;
    }

    static private Calendar getCalendar() {
        Calendar ret = Calendar.getInstance();
        ret.setTime(clock.getDate());
        return ret;
    }

    public void enableReindexMode(){
        reindex = true;
        ReindexOperations reindexOps = new ReindexOperations(root, definitionBuilder, definition.getIndexPath());
        definition = reindexOps.apply(indexDefnRewritten);
    }

    public long incIndexedNodes() {
        indexedNodes++;
        return indexedNodes;
    }

    boolean isAsyncIndexing() {
        return asyncIndexing;
    }

    public long getIndexedNodes() {
        return indexedNodes;
    }

    void indexUpdate() throws CommitFailedException {
        updateCallback.indexUpdate();
    }

    public IndexDefinition getDefinition() {
        return definition;
    }

    LuceneDocumentMaker newDocumentMaker(IndexDefinition.IndexingRule rule, String path){
        //Faceting is only enabled for async mode
        FacetsConfigProvider facetsConfigProvider = isAsyncIndexing() ? this : null;
        return new LuceneDocumentMaker(getTextExtractor(), facetsConfigProvider, augmentorFactory,
                definition, rule, path);
    }

    @Override
    public FacetsConfig getFacetsConfig() {
        if (facetsConfig == null){
            facetsConfig = FacetHelper.getFacetsConfig(definitionBuilder);
        }
        return facetsConfig;
    }

    private BinaryTextExtractor getTextExtractor(){
        if (textExtractor == null && isAsyncIndexing()){
            //Create lazily to ensure that if its reindex case then update definition is picked
            textExtractor = new BinaryTextExtractor(extractedTextCache, definition, reindex);
        }
        return textExtractor;
    }

    public boolean isReindex() {
        return reindex;
    }

    public static String configureUniqueId(NodeBuilder definition) {
        NodeBuilder status = definition.child(IndexDefinition.STATUS_NODE);
        String uid = status.getString(IndexDefinition.PROP_UID);
        if (uid == null) {
            try {
                uid = String.valueOf(Clock.SIMPLE.getTimeIncreasing());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                uid = String.valueOf(Clock.SIMPLE.getTime());
            }
            status.setProperty(IndexDefinition.PROP_UID, uid);
        }
        return uid;
    }

    private static IndexDefinition createIndexDefinition(NodeState root, NodeBuilder definition, IndexingContext
            indexingContext, boolean asyncIndexing) {
        NodeState defnState = definition.getBaseState();
        if (asyncIndexing && !IndexDefinition.isDisableStoredIndexDefinition()){
            if (definition.getBoolean(PROP_REFRESH_DEFN)){
                definition.removeProperty(PROP_REFRESH_DEFN);
                NodeState clonedState = NodeStateCloner.cloneVisibleState(defnState);
                definition.setChildNode(INDEX_DEFINITION_NODE, clonedState);
                log.info("Refreshed the index definition for [{}]", indexingContext.getIndexPath());
                if (log.isDebugEnabled()){
                    log.debug("Updated index definition is {}", NodeStateUtils.toString(clonedState));
                }
            } else if (!definition.hasChildNode(INDEX_DEFINITION_NODE)){
                definition.setChildNode(INDEX_DEFINITION_NODE, NodeStateCloner.cloneVisibleState(defnState));
                log.info("Stored the cloned index definition for [{}]. Changes in index definition would now only be " +
                                "effective post reindexing", indexingContext.getIndexPath());
            }
        }
        return new IndexDefinition(root, defnState,indexingContext.getIndexPath());
    }
}
