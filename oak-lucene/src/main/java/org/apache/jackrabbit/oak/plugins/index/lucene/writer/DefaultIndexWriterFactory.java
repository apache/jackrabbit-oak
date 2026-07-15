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
package org.apache.jackrabbit.oak.plugins.index.lucene.writer;

import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexWriterFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.DirectoryFactory;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

public class DefaultIndexWriterFactory implements LuceneIndexWriterFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultIndexWriterFactory.class);

    private final MountInfoProvider mountInfoProvider;
    private final DirectoryFactory directoryFactory;
    private final LuceneIndexWriterConfig writerConfig;
    private final IndexWriterPool indexWriterPool;

    public DefaultIndexWriterFactory(MountInfoProvider mountInfoProvider,
                                     DirectoryFactory directoryFactory,
                                     LuceneIndexWriterConfig writerConfig) {
        this(mountInfoProvider, directoryFactory, writerConfig, null);
    }

    public DefaultIndexWriterFactory(MountInfoProvider mountInfoProvider,
                                     DirectoryFactory directoryFactory,
                                     LuceneIndexWriterConfig writerConfig,
                                     IndexWriterPool indexWriterPool) {
        this.mountInfoProvider = requireNonNull(mountInfoProvider);
        this.directoryFactory = requireNonNull(directoryFactory);
        this.writerConfig = requireNonNull(writerConfig);
        this.indexWriterPool = indexWriterPool;
    }

    @Override
    public LuceneIndexWriter newInstance(IndexDefinition def, NodeBuilder definitionBuilder,
                                         CommitInfo commitInfo, boolean reindex) {
        Validate.checkArgument(def instanceof LuceneIndexDefinition,
                "Expected %s but found %s for index definition",
                LuceneIndexDefinition.class, def.getClass());

        LuceneIndexDefinition definition = (LuceneIndexDefinition) def;

        if (mountInfoProvider.hasNonDefaultMounts()) {
            return wrapWithPipelinedIndexWriter(
                    new MultiplexingIndexWriter(directoryFactory, mountInfoProvider, definition, definitionBuilder, reindex, writerConfig),
                    definition.getIndexName());
        }
        DefaultIndexWriter writer = new DefaultIndexWriter(definition, definitionBuilder, directoryFactory,
                FulltextIndexConstants.INDEX_DATA_CHILD_NAME,
                LuceneIndexConstants.SUGGEST_DATA_CHILD_NAME, reindex, writerConfig);

        return wrapWithPipelinedIndexWriter(writer, definition.getIndexName());
    }

    public void close() {
        LOG.debug("Closing LuceneIndexWriterFactory");
        if (indexWriterPool == null) {
            LOG.debug("Not using an Index writer pool");
        } else {
            indexWriterPool.close();
        }
    }

    private LuceneIndexWriter wrapWithPipelinedIndexWriter(LuceneIndexWriter writer, String indexName) {
        return indexWriterPool == null ? writer : new PooledLuceneIndexWriter(indexWriterPool, writer, indexName);
    }
}
