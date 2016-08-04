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

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.OakDirectory;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_FILE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_PATH;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.SUGGEST_DATA_CHILD_NAME;

public class DefaultIndexReaderFactory implements LuceneIndexReaderFactory {
    private final IndexCopier cloner;

    public DefaultIndexReaderFactory(@Nullable IndexCopier cloner) {
        this.cloner = cloner;
    }

    @Override
    public List<LuceneIndexReader> createReaders(IndexDefinition definition, NodeState defnState,
                                                 String indexPath) throws IOException {
        LuceneIndexReader reader = createReader(definition, defnState, indexPath,
                INDEX_DATA_CHILD_NAME, SUGGEST_DATA_CHILD_NAME);
        return reader != null ? ImmutableList.of(reader) : Collections.<LuceneIndexReader>emptyList();
    }

    @CheckForNull
    private LuceneIndexReader createReader(IndexDefinition definition, NodeState defnNodeState, String indexPath,
                                           String indexDataNodeName, String suggestDataNodeName) throws IOException {
        Directory directory = null;
        NodeState data = defnNodeState.getChildNode(indexDataNodeName);
        if (data.exists()) {
            directory = new OakDirectory(new ReadOnlyBuilder(defnNodeState), indexDataNodeName, definition, true);
            if (cloner != null) {
                directory = cloner.wrapForRead(indexPath, definition, directory, indexDataNodeName);
            }
        } else if (PERSISTENCE_FILE.equalsIgnoreCase(defnNodeState.getString(PERSISTENCE_NAME))) {
            String path = defnNodeState.getString(PERSISTENCE_PATH);
            if (path != null && new File(path).exists()) {
                directory = FSDirectory.open(new File(path));
            }
        }

        if (directory != null) {
            OakDirectory suggestDirectory = null;
            if (definition.isSuggestEnabled()) {
                suggestDirectory = new OakDirectory(new ReadOnlyBuilder(defnNodeState), suggestDataNodeName, definition, true);
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
