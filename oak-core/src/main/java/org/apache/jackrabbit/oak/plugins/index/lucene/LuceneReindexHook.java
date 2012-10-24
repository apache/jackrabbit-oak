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

import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.buildIndexDefinitions;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link CommitHook} that handles re-indexing and initial indexing of the
 * content.
 * 
 * Currently it triggers a full reindex on any detected index definition change
 * OR on the {@link IndexDefinition#isReindex()} flag
 * 
 */
public class LuceneReindexHook implements CommitHook, LuceneIndexConstants {

    private static final Logger LOG = LoggerFactory
            .getLogger(LuceneReindexHook.class);

    private final String indexConfigPath;

    public LuceneReindexHook(String indexConfigPath) {
        this.indexConfigPath = indexConfigPath;
    }

    @Override
    public NodeState processCommit(NodeState before, NodeState after)
            throws CommitFailedException {

        List<IndexDefinition> defsBefore = buildIndexDefinitions(before,
                indexConfigPath, TYPE_LUCENE);
        List<IndexDefinition> defsAfter = buildIndexDefinitions(after,
                indexConfigPath, TYPE_LUCENE);

        // TODO add a more fine-grained change verifier
        List<IndexDefinition> defsChanged = new ArrayList<IndexDefinition>();
        for (IndexDefinition def : defsAfter) {
            if (!defsBefore.contains(def)) {
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
            hooks.add(new LuceneEditor(def.getPath()));
        }
        NodeState done = CompositeHook.compose(hooks).processCommit(
                MemoryNodeState.EMPTY_NODE, after);
        LOG.debug("done reindexing repository content in {} ms.",
                System.currentTimeMillis() - t);
        return done;
    }
}
