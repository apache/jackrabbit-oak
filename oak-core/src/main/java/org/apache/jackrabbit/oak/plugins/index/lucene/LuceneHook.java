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
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class LuceneHook implements CommitHook, LuceneIndexConstants {

    private final String indexConfigPath;

    public LuceneHook(String indexConfigPath) {
        this.indexConfigPath = indexConfigPath;
    }

    @Override
    public NodeState processCommit(NodeState before, NodeState after)
            throws CommitFailedException {
        List<CommitHook> hooks = new ArrayList<CommitHook>();
        List<IndexDefinition> indexDefinitions = buildIndexDefinitions(after,
                indexConfigPath, TYPE_LUCENE);
        for (IndexDefinition def : indexDefinitions) {
            hooks.add(new LuceneEditor(def));
        }
        return CompositeHook.compose(hooks).processCommit(before, after);
    }

}
