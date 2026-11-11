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
package org.apache.jackrabbit.oak.plugins.index.luceneNg.internal;

import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.IndexNodeManager;
import org.apache.jackrabbit.oak.plugins.index.search.update.ReaderRefreshPolicy;
import org.jetbrains.annotations.NotNull;

/**
 * Wraps one generation of a {@link LuceneNgIndexNode} for {@link
 * org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexTracker}. A new manager (and
 * a new wrapped node) is constructed by the tracker's {@code openIndex} whenever the index
 * definition or storage changes — there is no in-place reopen, hence {@link
 * ReaderRefreshPolicy#NEVER}: NRT/hybrid indexing is an explicitly deferred feature (see
 * module README).
 */
public class LuceneNgIndexNodeManager extends IndexNodeManager<LuceneNgIndexNode> {

    private final String path;
    private final LuceneNgIndexNode indexNode;

    public LuceneNgIndexNodeManager(@NotNull String path, @NotNull LuceneNgIndexNode indexNode) {
        this.path = path;
        this.indexNode = indexNode;
        indexNode.bindOwner(this);
    }

    @Override
    protected String getName() {
        return path;
    }

    @Override
    protected LuceneNgIndexNode getIndexNode() {
        return indexNode;
    }

    @Override
    protected IndexDefinition getDefinition() {
        return indexNode.getDefinition();
    }

    @Override
    protected ReaderRefreshPolicy getReaderRefreshPolicy() {
        return ReaderRefreshPolicy.NEVER;
    }

    @Override
    protected void refreshReaders() {
        // Never invoked (ReaderRefreshPolicy.NEVER above never calls the refresh callback).
    }

    @Override
    protected void releaseResources() {
        indexNode.closeResources();
    }

    /**
     * Package-private wrapper around the inherited, {@code protected} {@code
     * IndexNodeManager.release()}: {@link LuceneNgIndexNode} isn't itself an {@code
     * IndexNodeManager} subclass, so it can't call the protected method directly across
     * packages. Same-package access here lets it do so via this class instead.
     */
    void releaseNode() {
        release();
    }
}
