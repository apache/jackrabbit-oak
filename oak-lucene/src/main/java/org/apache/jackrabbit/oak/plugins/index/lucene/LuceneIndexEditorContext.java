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

import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.EXCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_PATH;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.VERSION;
import static org.apache.lucene.store.NoLockFactory.getNoLockFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

public class LuceneIndexEditorContext {

    private static final Logger log = LoggerFactory
            .getLogger(LuceneIndexEditorContext.class);

    private static IndexWriterConfig getIndexWriterConfig(Analyzer analyzer) {
        // FIXME: Hack needed to make Lucene work in an OSGi environment
        Thread thread = Thread.currentThread();
        ClassLoader loader = thread.getContextClassLoader();
        thread.setContextClassLoader(IndexWriterConfig.class.getClassLoader());
        try {
            IndexWriterConfig config = new IndexWriterConfig(VERSION, analyzer);
            config.setMergeScheduler(new SerialMergeScheduler());
            config.setCodec(new OakCodec());
            return config;
        } finally {
            thread.setContextClassLoader(loader);
        }
    }

    private static Directory newIndexDirectory(NodeBuilder definition)
            throws IOException {
        String path = definition.getString(PERSISTENCE_PATH);
        if (path == null) {
            return new OakDirectory(definition.child(INDEX_DATA_CHILD_NAME));
        } else {
            // try {
            File file = new File(path);
            file.mkdirs();
            // TODO: close() is never called
            // TODO: no locking used
            // --> using the FS backend for the index is in any case
            // troublesome in clustering scenarios and for backup
            // etc. so instead of fixing these issues we'd better
            // work on making the in-content index work without
            // problems (or look at the Solr indexer as alternative)
            return FSDirectory.open(file, getNoLockFactory());
            // } catch (IOException e) {
            // throw new CommitFailedException("Lucene", 1,
            // "Failed to open the index in " + path, e);
            // }
        }
    }

    private final IndexWriterConfig config;

    private static final Parser parser = new AutoDetectParser();

    private final NodeBuilder definition;

    private IndexWriter writer = null;

    private final int propertyTypes;

    private final Set<String> excludes;

    private long indexedNodes;

    private final IndexUpdateCallback updateCallback;

    LuceneIndexEditorContext(NodeBuilder definition, Analyzer analyzer, IndexUpdateCallback updateCallback) {
        this.definition = definition;
        this.config = getIndexWriterConfig(analyzer);

        PropertyState pst = definition.getProperty(INCLUDE_PROPERTY_TYPES);
        if (pst != null) {
            int types = 0;
            for (String inc : pst.getValue(Type.STRINGS)) {
                try {
                    types |= 1 << PropertyType.valueFromName(inc);
                } catch (IllegalArgumentException e) {
                    log.warn("Unknown property type: " + inc);
                }
            }
            this.propertyTypes = types;
        } else {
            this.propertyTypes = -1;
        }
        PropertyState pse = definition.getProperty(EXCLUDE_PROPERTY_NAMES);
        if (pse != null) {
            excludes = ImmutableSet.copyOf(pse.getValue(Type.STRINGS));
        } else {
            excludes = ImmutableSet.of();
        }
        this.indexedNodes = 0;
        this.updateCallback = updateCallback;
    }

    int getPropertyTypes() {
        return propertyTypes;
    }

    boolean includeProperty(String name) {
        return !excludes.contains(name);
    }

    Parser getParser() {
        return parser;
    }

    IndexWriter getWriter() throws IOException {
        if (writer == null) {
            writer = new IndexWriter(newIndexDirectory(definition), config);
        }
        return writer;
    }

    /**
     * close writer if it's not null
     */
    void closeWriter() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }

    public long incIndexedNodes() {
        indexedNodes++;
        return indexedNodes;
    }

    public long getIndexedNodes() {
        return indexedNodes;
    }

    void indexUpdate() throws CommitFailedException {
        updateCallback.indexUpdate();
    }

}
