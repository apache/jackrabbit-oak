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

import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_FILE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_PATH;

import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

class IndexNode {

    static IndexNode open(String name, NodeState definition)
            throws IOException {
        Directory directory = null;

        NodeState data = definition.getChildNode(INDEX_DATA_CHILD_NAME);
        if (data.exists()) {
            directory = new OakDirectory(new ReadOnlyBuilder(data));
        } else if (PERSISTENCE_FILE.equalsIgnoreCase(definition.getString(PERSISTENCE_NAME))) {
            String path = definition.getString(PERSISTENCE_PATH);
            if (path != null && new File(path).exists()) {
                directory = FSDirectory.open(new File(path));
            }
        }

        if (directory != null) {
            try {
                IndexNode index = new IndexNode(name, definition, directory);
                directory = null; // closed in Index.close()
                return index;
            } finally {
                if (directory != null) {
                    directory.close();
                }
            }
        }

        return null;
    }

    private final String name;

    private final NodeState definition;

    private final Directory directory;

    private final IndexReader reader;

    private final IndexSearcher searcher;

    private int refcount = 0;

    private boolean closed = false;

    IndexNode(String name, NodeState definition, Directory directory)
            throws IOException {
        this.name = name;
        this.definition = definition;
        this.directory = directory;
        this.reader = DirectoryReader.open(directory);
        this.searcher = new IndexSearcher(reader);
    }

    String getName() {
        return name;
    }

    NodeState getDefinition() {
        return definition;
    }

    synchronized IndexSearcher acquireSearcher() {
        checkState(!closed);
        refcount++;
        return searcher;
    }

    synchronized void releaseSearcher() throws IOException {
        refcount--;
        if (closed && refcount == 0) {
            reallyClose();
        }
    }

    synchronized void close() throws IOException {
        closed = true;
        if (refcount == 0) {
            reallyClose();
        }
    }

    private void reallyClose() throws IOException {
        try {
            reader.close();
        } finally {
            directory.close();
        }
    }

}
