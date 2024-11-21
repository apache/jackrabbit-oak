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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.StreamSupport;

import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.DirectoryFactory;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.index.IndexableField;

import static org.apache.jackrabbit.guava.common.collect.Iterables.concat;
import static java.util.Collections.singleton;

class MultiplexingIndexWriter implements LuceneIndexWriter {

    private final MountInfoProvider mountInfoProvider;
    private final DirectoryFactory directoryFactory;
    private final LuceneIndexDefinition definition;
    private final NodeBuilder definitionBuilder;
    private final boolean reindex;
    private final LuceneIndexWriterConfig writerConfig;

    private final Map<Mount, DefaultIndexWriter> writers = new ConcurrentHashMap<>();

    public MultiplexingIndexWriter(DirectoryFactory directoryFactory, MountInfoProvider mountInfoProvider,
                                   LuceneIndexDefinition definition, NodeBuilder definitionBuilder,
                                   boolean reindex, LuceneIndexWriterConfig writerConfig) {
        this.mountInfoProvider = mountInfoProvider;
        this.definition = definition;
        this.definitionBuilder = definitionBuilder;
        this.reindex = reindex;
        this.directoryFactory = directoryFactory;
        this.writerConfig = writerConfig;
    }

    @Override
    public void updateDocument(String path, Iterable<? extends IndexableField> doc) throws IOException {
        getWriter(path).updateDocument(path, doc);
    }

    @Override
    public void deleteDocuments(String path) throws IOException {
        Mount mount = mountInfoProvider.getMountByPath(path);
        getWriter(mount).deleteDocuments(path);

        //In case of default mount look for other mounts with roots under this path
        //Note that one mount cannot be part of another mount
        if (mount.isDefault()) {
            //If any mount falls under given path then delete all documents in that
            for (Mount m : mountInfoProvider.getMountsPlacedUnder(path)) {
                getWriter(m).deleteAll();
            }
        }
    }

    @Override
    public boolean close(long timestamp) throws IOException {
        // explicitly get writers for mounts which haven't got writers even at close.
        // This essentially ensures we respect DefaultIndexWriters#close's intent to
        // create empty index even if nothing has been written during re-index.
        StreamSupport.stream(concat(singleton(mountInfoProvider.getDefaultMount()), mountInfoProvider.getNonDefaultMounts())
                .spliterator(), false)
                .filter(m ->  reindex && !m.isReadOnly()) // only needed when re-indexing for read-write mounts.
                                                         // reindex for ro-mount doesn't make sense in this case anyway.
                .forEach(this::getWriter); // open default writers for mounts that passed all our tests

        boolean indexUpdated = false;
        for (LuceneIndexWriter w : writers.values()) {
            indexUpdated |= w.close(timestamp);
        }
        return indexUpdated;
    }

    private LuceneIndexWriter getWriter(String path) {
        Mount mount = mountInfoProvider.getMountByPath(path);
        return getWriter(mount);
    }

    private DefaultIndexWriter getWriter(Mount mount) {
        DefaultIndexWriter writer = writers.computeIfAbsent(mount, w -> createWriter(mount));
        return writer;
    }

    private DefaultIndexWriter createWriter(Mount m) {
        String dirName = MultiplexersLucene.getIndexDirName(m);
        String suggestDirName = MultiplexersLucene.getSuggestDirName(m);
        return new DefaultIndexWriter(definition, definitionBuilder, directoryFactory, dirName,
            suggestDirName, reindex, writerConfig);
    }

}
