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
package org.apache.jackrabbit.oak.plugins.document.prefetch;

import java.util.LinkedList;
import java.util.List;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

public class CacheWarming {

    private static final Logger LOG = LoggerFactory.getLogger(CacheWarming.class);

    private final DocumentStore store;

    public CacheWarming(DocumentStore store) {
        this.store = store;
    }

    public void prefetch(@NotNull Iterable<String> paths,
                         @NotNull DocumentNodeState rootState) {
        requireNonNull(paths);
        requireNonNull(rootState);

        List<String> ids = new LinkedList<>();
        for (String aPath : paths) {
            if (!isCached(aPath, rootState)) {
                String id = Utils.getIdFromPath(aPath);
                ids.add(id);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Prefetch {} nodes", ids.size());
        }
        store.prefetch(Collection.NODES, ids);
    }

    private boolean isCached(String path, DocumentNodeState rootState) {
        if (rootState == null) {
            // don't know
            return false;
        }
        DocumentNodeState n = rootState;
        for(String e : PathUtils.elements(path)) {
            if (!n.exists() || n.hasNoChildren()) {
                // No need to check further down the path.
                // Descendants do not exist. We don't gain anything
                // by reading them from the store.
                break;
            }
            n = n.getChildIfCached(e);
            if (n == null) {
                return false;
            }
        }
        return true;
    }
}
