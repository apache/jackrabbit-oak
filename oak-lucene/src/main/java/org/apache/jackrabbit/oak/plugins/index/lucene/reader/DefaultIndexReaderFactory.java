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

package org.apache.jackrabbit.oak.plugins.index.lucene.reader;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.OakDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.MultiplexersLucene;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.jetbrains.annotations.Nullable;

import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.SUGGEST_DATA_CHILD_NAME;

public class DefaultIndexReaderFactory implements LuceneIndexReaderFactory {
    private final IndexCopier cloner;
    private final MountInfoProvider mountInfoProvider;

    public DefaultIndexReaderFactory(MountInfoProvider mountInfoProvider, @Nullable IndexCopier cloner) {
        this.cloner = cloner;
        this.mountInfoProvider = mountInfoProvider;
    }

    @Override
    public List<LuceneIndexReader> createReaders(LuceneIndexDefinition definition, NodeState defnState,
                                                 String indexPath) throws IOException {
        if (!mountInfoProvider.hasNonDefaultMounts()) {
            LuceneIndexReader reader = createReader(definition, defnState, indexPath,
                    FulltextIndexConstants.INDEX_DATA_CHILD_NAME, SUGGEST_DATA_CHILD_NAME);
            return reader != null ? ImmutableList.of(reader) : Collections.<LuceneIndexReader>emptyList();
        } else {
            return createMountedReaders(definition, defnState, indexPath);
        }
    }

    private List<LuceneIndexReader> createMountedReaders(LuceneIndexDefinition definition, NodeState defnState, String
            indexPath) throws IOException {
        ImmutableList.Builder<LuceneIndexReader> readers = ImmutableList.builder();
        LuceneIndexReader reader = createReader(mountInfoProvider.getDefaultMount(), definition, defnState, indexPath);
        //Default mount is the first entry. This ensures that suggester, spellcheck can work on that untill they
        //support multiple readers
        if (reader != null) {
            readers.add(reader);
        }
        for (Mount m : mountInfoProvider.getNonDefaultMounts()) {
            reader = createReader(m, definition, defnState, indexPath);
            // Although, it shouldn't matter much, but let's not add extra readers if they don't have any docs
            if (reader != null && reader.getReader() != null && reader.getReader().numDocs() > 0) {
                readers.add(reader);
            }
        }
        return readers.build();
    }

    @Nullable
    private LuceneIndexReader createReader(Mount mount, LuceneIndexDefinition definition, NodeState defnNodeState,
                                           String indexPath) throws IOException {
        return createReader(definition, defnNodeState, indexPath, MultiplexersLucene.getIndexDirName(mount),
                MultiplexersLucene.getSuggestDirName(mount));
    }

    @Nullable
    private LuceneIndexReader createReader(LuceneIndexDefinition definition, NodeState defnNodeState, String indexPath,
                                           String indexDataNodeName, String suggestDataNodeName) throws IOException {
        Directory directory = null;
        NodeState data = defnNodeState.getChildNode(indexDataNodeName);
        if (data.exists()) {
            directory = new OakDirectory(new ReadOnlyBuilder(defnNodeState), indexDataNodeName, definition, true);
            if (cloner != null) {
                directory = cloner.wrapForRead(indexPath, definition, directory, indexDataNodeName);
            }
        } else if (FulltextIndexConstants.PERSISTENCE_FILE.equalsIgnoreCase(
                defnNodeState.getString(FulltextIndexConstants.PERSISTENCE_NAME))) {
            String path = defnNodeState.getString(FulltextIndexConstants.PERSISTENCE_PATH);
            if (path != null && new File(path).exists()) {
                directory = FSDirectory.open(new File(path));
            }
        }

        if (directory != null) {
            Directory suggestDirectory = null;
            if (definition.isSuggestEnabled()) {
                suggestDirectory = new OakDirectory(new ReadOnlyBuilder(defnNodeState), suggestDataNodeName, definition, true);
                if (cloner != null && definition.getUniqueId() != null) {
                    suggestDirectory = cloner.wrapForRead(indexPath, definition, suggestDirectory, suggestDataNodeName);
                }
            }

            try{
                LuceneIndexReader reader = new DefaultIndexReader(directory, suggestDirectory, definition.getAnalyzer());
                directory = null; // closed in LuceneIndexReader.close()
                return reader;
            } finally {
                if (directory != null){
                    directory.close();
                }
            }
        }
        return null;
    }
}
