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
package org.apache.jackrabbit.oak.plugins.lucene;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.query.IndexDefinition;
import org.apache.jackrabbit.oak.spi.query.IndexUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link CommitHook} that handles re-indexing and initial indexing of the
 * content.
 * 
 * Currently it triggers a full reindex on any detected index definition change
 * (excepting the properties) OR on removing the
 * {@link LuceneIndexConstants#INDEX_UPDATE} property
 * 
 * Warning: This hook has to be placed before the updater {@link LuceneHook},
 * otherwise it is going to miss the {@link LuceneIndexConstants#INDEX_UPDATE}
 * change to null
 * 
 */
public class LuceneReindexHook implements CommitHook, LuceneIndexConstants {

    private static final Logger LOG = LoggerFactory
            .getLogger(LuceneReindexHook.class);

    private final String indexConfigPath;

    public LuceneReindexHook(String indexConfigPath) {
        this.indexConfigPath = indexConfigPath;
    }

    /**
     * TODO test only
     */
    public LuceneReindexHook() {
        this(IndexUtils.DEFAULT_INDEX_HOME);
    }

    @Override
    public NodeState processCommit(NodeState before, NodeState after)
            throws CommitFailedException {

        List<IndexDefinition> defsBefore = IndexUtils.buildIndexDefinitions(
                before, indexConfigPath, TYPE);
        List<IndexDefinition> defsAfter = IndexUtils.buildIndexDefinitions(
                after, indexConfigPath, TYPE);

        // TODO add a more fine-grained change verifier
        List<IndexDefinition> defsChanged = new ArrayList<IndexDefinition>();
        for (IndexDefinition def : defsAfter) {
            if (!defsBefore.contains(def)) {
                defsChanged.add(def);
            }
            // verify initial state or forced reindex
            if (def.getProperties().get(INDEX_UPDATE) == null) {
                defsChanged.add(def);
            }
        }
        if (defsChanged.isEmpty()) {
            return after;
        }
        LOG.debug("found {} updated index definitions ({})",
                defsChanged.size(), defsChanged);
        LOG.debug("reindexing repository content");
        long t = System.currentTimeMillis();
        // TODO buffer content reindex
        List<CommitHook> hooks = new ArrayList<CommitHook>();
        for (IndexDefinition def : defsChanged) {
            hooks.add(new LuceneEditor(def));
        }
        NodeState done = CompositeHook.compose(hooks).processCommit(null, after);
        LOG.debug("done reindexing repository content in {} ms.",
                System.currentTimeMillis() - t);
        return done;
    }
}
