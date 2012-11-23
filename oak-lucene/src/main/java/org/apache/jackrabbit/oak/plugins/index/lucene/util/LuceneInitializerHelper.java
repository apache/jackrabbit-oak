/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.lucene.util;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;

public class LuceneInitializerHelper implements RepositoryInitializer {

    private final String path;

    public LuceneInitializerHelper(String path) {
        this.path = path;
    }

    @Override
    public void initialize(NodeStore store) {
        boolean dirty = false;

        NodeStoreBranch branch = store.branch();
        NodeBuilder root = branch.getRoot().builder();

        NodeBuilder index = root;
        for (String p : PathUtils.elements(path)) {
            if (!index.hasChildNode(p)) {
                dirty = true;
            }
            index = index.child(p);
        }

        if (dirty) {
            index.setProperty(JcrConstants.JCR_PRIMARYTYPE,
                    "oak:queryIndexDefinition", Type.NAME).setProperty("type",
                    "lucene");
            index.setProperty("reindex", true);
            try {
                branch.setRoot(root.getNodeState());
                branch.merge();
            } catch (CommitFailedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
