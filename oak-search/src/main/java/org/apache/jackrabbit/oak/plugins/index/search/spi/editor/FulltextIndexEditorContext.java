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
package org.apache.jackrabbit.oak.plugins.index.search.spi.editor;

import java.io.IOException;
import java.util.Calendar;
import java.util.UUID;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexingContext;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.search.ReindexOperations;
import org.apache.jackrabbit.oak.plugins.index.search.spi.binary.FulltextBinaryTextExtractor;
import org.apache.jackrabbit.oak.plugins.index.search.util.NodeStateCloner;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.util.ISO8601;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_RANDOM_SEED;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_REFRESH_DEFN;
import static org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.INDEX_DEFINITION_NODE;

/**
 *
 */
public abstract class FulltextIndexEditorContext<D> {

  private static final Logger log = LoggerFactory
      .getLogger(FulltextIndexEditorContext.class);

  private static final PerfLogger PERF_LOGGER =
      new PerfLogger(LoggerFactory.getLogger(FulltextIndexEditorContext.class.getName() + ".perf"));

  protected IndexDefinition definition;

  protected final NodeBuilder definitionBuilder;

  private final FulltextIndexWriterFactory<D> indexWriterFactory;

  private FulltextIndexWriter<D> writer = null;

  private long indexedNodes;

  private final IndexUpdateCallback updateCallback;

  private boolean reindex;

  private final ExtractedTextCache extractedTextCache;

  private final NodeState root;

  private final IndexingContext indexingContext;

  private final boolean asyncIndexing;

  //Intentionally static, so that it can be set without passing around clock objects
  //Set for testing ONLY
  private static Clock clock = Clock.SIMPLE;

  private final boolean indexDefnRewritten;

  private FulltextBinaryTextExtractor textExtractor;

  private PropertyUpdateCallback propertyUpdateCallback;

  protected FulltextIndexEditorContext(NodeState root, NodeBuilder definition,
                                       @Nullable IndexDefinition indexDefinition,
                                       IndexUpdateCallback updateCallback,
                                       FulltextIndexWriterFactory indexWriterFactory,
                                       ExtractedTextCache extractedTextCache,
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
    this.asyncIndexing = asyncIndexing;
    if (this.definition.isOfOldFormat()){
      indexDefnRewritten = true;
      IndexDefinition.updateDefinition(definition, indexingContext.getIndexPath());
    } else {
      indexDefnRewritten = false;
    }
  }

  public abstract IndexDefinition.Builder newDefinitionBuilder();

  public abstract DocumentMaker<D> newDocumentMaker(IndexDefinition.IndexingRule rule, String path);

  protected FulltextBinaryTextExtractor createBinaryTextExtractor(ExtractedTextCache extractedTextCache,
                                                                  IndexDefinition definition, boolean reindex) {
    return new FulltextBinaryTextExtractor(extractedTextCache, definition, reindex);
  }

  public FulltextIndexWriter<D> getWriter() throws IOException {
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

  @Nullable
  public PropertyUpdateCallback getPropertyUpdateCallback() {
    return propertyUpdateCallback;
  }

  public void setPropertyUpdateCallback(PropertyUpdateCallback propertyUpdateCallback) {
    this.propertyUpdateCallback = propertyUpdateCallback;
  }

  /**
   * close writer if it's not null
   */
  public void closeWriter() throws IOException {
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
  protected static void setClock(Clock c) {
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
    ReindexOperations reindexOps =
            new ReindexOperations(root, definitionBuilder, definition.getIndexPath(), newDefinitionBuilder());
    definition = reindexOps.apply(indexDefnRewritten);
  }

  public long incIndexedNodes() {
    indexedNodes++;
    return indexedNodes;
  }

  public boolean isAsyncIndexing() {
    return asyncIndexing;
  }

  public long getIndexedNodes() {
    return indexedNodes;
  }

  public void indexUpdate() throws CommitFailedException {
    updateCallback.indexUpdate();
  }

  public IndexDefinition getDefinition() {
    return definition;
  }

  protected FulltextBinaryTextExtractor getTextExtractor(){
    if (textExtractor == null && isAsyncIndexing()){
      //Create lazily to ensure that if its reindex case then update definition is picked
      textExtractor = createBinaryTextExtractor(extractedTextCache, definition, reindex);
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

  private IndexDefinition createIndexDefinition(NodeState root, NodeBuilder definition, IndexingContext
      indexingContext, boolean asyncIndexing) {
    NodeState defnState = definition.getBaseState();
    if (asyncIndexing) {
      // A good time to check and see if we want to inject our random
      // seed - required due to OAK-7929 but could be useful otherwise too.
      Long defRandom = getLongPropertyOrNull(definition.getProperty(PROP_RANDOM_SEED));
      if (defRandom == null) {
        long seed = UUID.randomUUID().getMostSignificantBits();
        definition.setProperty(PROP_RANDOM_SEED, seed);
        defRandom = seed;
      }

      if (!IndexDefinition.isDisableStoredIndexDefinition()) {
        if (definition.getBoolean(PROP_REFRESH_DEFN)) {
          definition.removeProperty(PROP_REFRESH_DEFN);
          NodeState clonedState = NodeStateCloner.cloneVisibleState(defnState);
          definition.setChildNode(INDEX_DEFINITION_NODE, clonedState);
          log.info("Refreshed the index definition for [{}]", indexingContext.getIndexPath());
          if (log.isDebugEnabled()) {
            log.debug("Updated index definition is {}", NodeStateUtils.toString(clonedState));
          }
        } else if (!definition.hasChildNode(INDEX_DEFINITION_NODE)) {
          definition.setChildNode(INDEX_DEFINITION_NODE, NodeStateCloner.cloneVisibleState(defnState));
          log.info("Stored the cloned index definition for [{}]. Changes in index definition would now only be " +
                  "effective post reindexing", indexingContext.getIndexPath());
        } else {
          // This is neither reindex nor refresh. So, let's update cloned def with random seed
          // if it doesn't match what's there in main definition
          // Reindex would require another indexing cycle to update cloned def.
          // Refresh would anyway is going to do a real clone so that's ok
          NodeState clonedState = defnState.getChildNode(INDEX_DEFINITION_NODE);
          Long clonedRandom = getLongPropertyOrNull(clonedState.getProperty(PROP_RANDOM_SEED));

          if (clonedRandom == null || clonedRandom != defRandom) {
            definition.getChildNode(INDEX_DEFINITION_NODE).setProperty(PROP_RANDOM_SEED, defRandom);
          }
        }
      }
    }
    return newDefinitionBuilder()
            .root(root)
            .defn(defnState)
            .indexPath(indexingContext.getIndexPath())
            .build();
  }

  private Long getLongPropertyOrNull(PropertyState propertyState) {
    Long ret = null;

    if (propertyState != null) {
      try {
        ret = propertyState.getValue(Type.LONG);
      } catch (Exception e) {
        // It's ok to ignore the exception, but let's log it to help debugging.
        log.debug("Error occurred while reading property as long.", e);
      }
    }

    return ret;
  }
}
