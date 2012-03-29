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

package org.apache.jackrabbit.oak.jcr.state;

import org.apache.commons.collections.map.LRUMap;
import org.apache.jackrabbit.oak.jcr.SessionContext;
import org.apache.jackrabbit.oak.jcr.SessionImpl;
import org.apache.jackrabbit.oak.jcr.configuration.RepositoryConfiguration;
import org.apache.jackrabbit.oak.jcr.state.ChangeTree.NodeDelta;
import org.apache.jackrabbit.oak.jcr.util.Path;
import org.apache.jackrabbit.oak.jcr.util.Unchecked;

import java.util.Map;

public class NodeStateProvider {
    private final SessionContext<SessionImpl> sessionContext;
    private final TransientSpace transientSpace;
    private final Map<Path, TransientNodeState> cache;

    public NodeStateProvider(SessionContext<SessionImpl> sessionContext, TransientSpace transientSpace) {
        this.sessionContext = sessionContext;
        this.transientSpace = transientSpace;

        int cacheSize = 0;
        try {
            RepositoryConfiguration config =
                    sessionContext.getGlobalContext().getInstance(
                            RepositoryConfiguration.class);
            cacheSize = config.getNodeStateCacheSize();
        } catch (IllegalStateException ignore) {
            // FIXME: Shouldn't use the context to pass around configuration
        }

        if (cacheSize <= 0) {
            cache = null;
        } else {
            cache = Unchecked.cast(new LRUMap(cacheSize));
        }
    }

    public TransientNodeState getNodeState(Path path) {
        TransientNodeState state = cache == null ? null : cache.get(path);
        if (state == null) {
            NodeDelta delta = transientSpace.getNodeDelta(path);
            if (delta == null) {
                return null;
            }
            state = new TransientNodeState(sessionContext, delta);
            if (cache != null) {
                cache.put(path, state);
            }
        }
        return state;
    }

    public void release(Path path) {
        if (cache != null) {
            cache.remove(path);
        }
    }

    public void clear() {
        if (cache != null) {
            cache.clear();
        }
    }

    //------------------------------------------< internal/private >---

    TransientNodeState getNodeState(NodeDelta nodeDelta) {
        Path path = nodeDelta.getPath();
        TransientNodeState state = cache == null ? null : cache.get(path);
        if (state == null) {
            state = new TransientNodeState(sessionContext, nodeDelta);
            if (cache != null) {
                cache.put(path, state);
            }
        }
        return state;
    }

    NodeDelta getNodeDelta(Path path) {
        return transientSpace.getNodeDelta(path);
    }

}
