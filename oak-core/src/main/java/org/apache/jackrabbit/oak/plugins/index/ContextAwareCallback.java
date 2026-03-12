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

package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

/**
 * Extension to IndexUpdateCallback which also provides access to
 * {@link IndexingContext} and the root {@link NodeBuilder} for the current commit.
 */
public interface ContextAwareCallback extends IndexUpdateCallback {

    IndexingContext getIndexingContext();

    /**
     * Returns the root {@link NodeBuilder} for the current commit, allowing
     * index editors to write data outside the index definition subtree
     * (e.g. to {@code /var/indexing/lucene/<indexName>}).
     *
     * @return the root NodeBuilder, or {@code null} when not available
     *         (e.g. in test contexts where a plain mock is used)
     */
    default NodeBuilder getRootBuilder() {
        return null;
    }
}
