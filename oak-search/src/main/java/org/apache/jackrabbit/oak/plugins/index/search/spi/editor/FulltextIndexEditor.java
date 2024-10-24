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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexEditor;
import org.apache.jackrabbit.oak.plugins.index.search.Aggregate;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyUpdateCallback;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.commons.PathUtils.concat;

/**
 * Generic implementation of an {@link IndexEditor} which supports index time aggregation.
 */
public class FulltextIndexEditor<D> implements IndexEditor, Aggregate.AggregateRoot {

  private static final Logger log =
      LoggerFactory.getLogger(FulltextIndexEditor.class);

  public static final String TEXT_EXTRACTION_ERROR = "TextExtractionError";

  private final FulltextIndexEditorContext<D> context;

  /* Name of this node, or {@code null} for the root node. */
  private final String name;

  /* Parent editor or {@code null} if this is the root editor. */
  private final FulltextIndexEditor<D> parent;

  /* Path of this editor, built lazily in {@link #getPath()}. */
  private String path;

  private boolean propertiesChanged = false;

  private final List<PropertyState> propertiesModified = new ArrayList<>();

  /*
   * Flag indicating if the current tree being traversed has a deleted parent.
   */
  private final boolean isDeleted;

  private IndexDefinition.IndexingRule indexingRule;

  private List<Aggregate.Matcher> currentMatchers = Collections.emptyList();

  private final MatcherState matcherState;

  private final PathFilter.Result pathFilterResult;

  public FulltextIndexEditor(FulltextIndexEditorContext<D> context) {
    this.parent = null;
    this.name = null;
    this.path = "/";
    this.context = context;
    this.isDeleted = false;
    this.matcherState = MatcherState.NONE;
    this.pathFilterResult = context.getDefinition().getPathFilter().filter(PathUtils.ROOT_PATH);
  }

  public FulltextIndexEditor(FulltextIndexEditor<D> parent, String name,
                             MatcherState matcherState,
                             PathFilter.Result pathFilterResult,
                             boolean isDeleted) {
    this.parent = parent;
    this.name = name;
    this.path = null;
    this.context = parent.context;
    this.isDeleted = isDeleted;
    this.matcherState = matcherState;
    this.pathFilterResult = pathFilterResult;
  }

  public String getPath() {
    if (path == null) { // => parent != null
      path = concat(parent.getPath(), name);
    }
    return path;
  }

  @Override
  public void enter(NodeState before, NodeState after) {
    if (EmptyNodeState.MISSING_NODE == before && parent == null){
      context.enableReindexMode();
    }

    //Only check for indexing if the result is include.
    //In case like TRAVERSE nothing needs to be indexed for those
    //path
    if (pathFilterResult == PathFilter.Result.INCLUDE) {
      //For traversal in deleted sub tree before state has to be used
      NodeState current = after.exists() ? after : before;
      indexingRule = getDefinition().getApplicableIndexingRule(current);

      if (indexingRule != null) {
        currentMatchers = indexingRule.getAggregate().createMatchers(this);
      }
    }
  }

  @Override
  public void leave(NodeState before, NodeState after)
      throws CommitFailedException {
    if (propertiesChanged || !before.exists()) {
      String path = getPath();
      if (addOrUpdate(path, after, before.exists())) {
        long indexed = context.incIndexedNodes();
        if (indexed % 1000 == 0) {
          log.debug("[{}] => Indexed {} nodes...", getIndexName(), indexed);
        }
      }
    }

    for (Aggregate.Matcher m : matcherState.affectedMatchers){
      m.markRootDirty();
    }

    if (parent == null) {
      PropertyUpdateCallback callback = context.getPropertyUpdateCallback();
      if (callback != null) {
        callback.done();
      }

      try {
        context.closeWriter();
      } catch (IOException e) {
        CommitFailedException ce = new CommitFailedException("Fulltext", 4,
            "Failed to close the Fulltext index " + context.getIndexingContext().getIndexPath(), e);
        context.getIndexingContext().indexUpdateFailed(ce);
        throw ce;
      }
      if (context.getIndexedNodes() > 0) {
        log.debug("[{}] => Indexed {} nodes, done.", getIndexName(), context.getIndexedNodes());
      }
    }
  }

  @Override
  public void propertyAdded(PropertyState after) {
    markPropertyChanged(after.getName());
    checkAggregates(after.getName());
    propertyUpdated(null, after);
  }

  @Override
  public void propertyChanged(PropertyState before, PropertyState after) {
    markPropertyChanged(before.getName());
    propertiesModified.add(before);
    checkAggregates(before.getName());
    propertyUpdated(before, after);
  }

  @Override
  public void propertyDeleted(PropertyState before) {
    markPropertyChanged(before.getName());
    propertiesModified.add(before);
    checkAggregates(before.getName());
    propertyUpdated(before, null);
  }

  @Override
  public Editor childNodeAdded(String name, NodeState after) {
    PathFilter.Result filterResult = getPathFilterResult(name);
    if (filterResult != PathFilter.Result.EXCLUDE) {
      return new FulltextIndexEditor<>(this, name, getMatcherState(name, after), filterResult, false);
    }
    return null;
  }

  @Override
  public Editor childNodeChanged(
      String name, NodeState before, NodeState after) {
    PathFilter.Result filterResult = getPathFilterResult(name);
    if (filterResult != PathFilter.Result.EXCLUDE) {
      return new FulltextIndexEditor<>(this, name, getMatcherState(name, after), filterResult, false);
    }
    return null;
  }

  @Override
  public Editor childNodeDeleted(String name, NodeState before)
      throws CommitFailedException {
    PathFilter.Result filterResult = getPathFilterResult(name);
    if (filterResult == PathFilter.Result.EXCLUDE) {
      return null;
    }

    if (!isDeleted) {
      // tree deletion is handled on the parent node
      String path = concat(getPath(), name);
      try {
        FulltextIndexWriter<D> writer = context.getWriter();
        // Remove all index entries in the removed subtree
        writer.deleteDocuments(path);
        this.context.indexUpdate();
      } catch (IOException e) {
        CommitFailedException ce = new CommitFailedException("Fulltext", 5, "Failed to remove the index entries of"
            + " the removed subtree " + path + "for index " + context.getIndexingContext().getIndexPath(), e);
        context.getIndexingContext().indexUpdateFailed(ce);
        throw ce;
      }
    }

    MatcherState ms = getMatcherState(name, before);
    if (!ms.isEmpty()){
      return new FulltextIndexEditor<>(this, name, ms, filterResult, true);
    }
    return null; // no need to recurse down the removed subtree
  }

  public FulltextIndexEditorContext<D> getContext() {
    return context;
  }

  private boolean addOrUpdate(String path, NodeState state, boolean isUpdate)
      throws CommitFailedException {
    try {
      D d = makeDocument(path, state, isUpdate);
      if (d != null) {
        if (log.isTraceEnabled()) {
          log.trace("[{}] Indexed document for {} is {}", getIndexName(), path, d);
        }
        context.indexUpdate();
        context.getWriter().updateDocument(path, d);
        return true;
      }
    } catch (IOException e) {
      log.warn("Failed to index the node [{}] due to {}", path, e.getMessage());
      CommitFailedException ce = new CommitFailedException("Fulltext", 3,
          "Failed to index the node " + path, e);
      context.getIndexingContext().indexUpdateFailed(ce);
      throw ce;
    } catch (IllegalArgumentException ie) {
      log.warn("Failed to index the node [{}]", path, ie);
    }
    return false;
  }

  private D makeDocument(String path, NodeState state, boolean isUpdate) throws IOException {
    if (!isIndexable()) {
      return null;
    }
    return context.newDocumentMaker(indexingRule, path).makeDocument(state, isUpdate, propertiesModified);
  }


  //~-------------------------------------------------------< Aggregate >

  @Override
  public void markDirty() {
    propertiesChanged = true;
  }

  private MatcherState getMatcherState(String name, NodeState after) {
    List<Aggregate.Matcher> matched = new ArrayList<>();
    List<Aggregate.Matcher> inherited = new ArrayList<>();
    for (Aggregate.Matcher m : Iterables.concat(matcherState.inherited, currentMatchers)) {
      Aggregate.Matcher result = m.match(name, after);
      if (result.getStatus() == Aggregate.Matcher.Status.MATCH_FOUND){
        matched.add(result);
      }

      if (result.getStatus() != Aggregate.Matcher.Status.FAIL){
        inherited.addAll(result.nextSet());
      }
    }

    if (!matched.isEmpty() || !inherited.isEmpty()) {
      return new MatcherState(matched, inherited);
    }
    return MatcherState.NONE;
  }


  /*
   * Determines which all matchers are affected by this property change
   *
   * @param name modified property name
   */
  private void checkAggregates(String name) {
    for (Aggregate.Matcher m : matcherState.matched) {
      if (!matcherState.affectedMatchers.contains(m)
          && m.aggregatesProperty(name)) {
        matcherState.affectedMatchers.add(m);
      }
    }
  }

  static class MatcherState {
    final static MatcherState NONE = new MatcherState(Collections.<Aggregate.Matcher>emptyList(),
        Collections.<Aggregate.Matcher>emptyList());

    final List<Aggregate.Matcher> matched;
    final List<Aggregate.Matcher> inherited;
    final Set<Aggregate.Matcher> affectedMatchers;

    public MatcherState(List<Aggregate.Matcher> matched,
                        List<Aggregate.Matcher> inherited){
      this.matched = matched;
      this.inherited = inherited;

      //Affected matches would only be used when there are
      //some matched matchers
      if (matched.isEmpty()){
        affectedMatchers = Collections.emptySet();
      } else {
        affectedMatchers = CollectionUtils.newIdentityHashSet();
      }
    }

    public boolean isEmpty() {
      return matched.isEmpty() && inherited.isEmpty();
    }
  }

  private void markPropertyChanged(String name) {
    if (isIndexable()
        && !propertiesChanged
        && indexingRule.isIndexed(name)) {
      propertiesChanged = true;
    }
  }

  private void propertyUpdated(PropertyState before, PropertyState after) {
    PropertyUpdateCallback callback = context.getPropertyUpdateCallback();

    //Avoid further work if no callback is present
    if (callback == null) {
      return;
    }

    String propertyName = before != null ? before.getName() : after.getName();

    if (isIndexable()) {
      PropertyDefinition pd = indexingRule.getConfig(propertyName);
      if (pd != null) {
        callback.propertyUpdated(getPath(), propertyName, pd, before, after);
      }
    }

    for (Aggregate.Matcher m : matcherState.matched) {
      if (m.aggregatesProperty(propertyName)) {
        Aggregate.Include i = m.getCurrentInclude();
        if (i instanceof Aggregate.PropertyInclude) {
          PropertyDefinition pd = ((Aggregate.PropertyInclude) i).getPropertyDefinition();
          String propertyRelativePath = PathUtils.concat(m.getMatchedPath(), propertyName);

          callback.propertyUpdated(m.getRootPath(), propertyRelativePath, pd, before, after);
        }
      }
    }
  }

  private IndexDefinition getDefinition() {
    return context.getDefinition();
  }

  private boolean isIndexable(){
    return indexingRule != null;
  }

  private PathFilter.Result getPathFilterResult(String childNodeName) {
    return context.getDefinition().getPathFilter().filter(concat(getPath(), childNodeName));
  }

  private String getIndexName() {
    return context.getDefinition().getIndexName();
  }
}
