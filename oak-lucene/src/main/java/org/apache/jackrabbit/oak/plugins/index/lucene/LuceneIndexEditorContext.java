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

import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_PATH;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.VERSION;
import static org.apache.lucene.store.NoLockFactory.getNoLockFactory;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.util.ISO8601;
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

public class LuceneIndexEditorContext {

    private static final Logger log = LoggerFactory
            .getLogger(LuceneIndexEditorContext.class);

    private static IndexWriterConfig getIndexWriterConfig(Analyzer analyzer, IndexDefinition definition) {
        // FIXME: Hack needed to make Lucene work in an OSGi environment
        Thread thread = Thread.currentThread();
        ClassLoader loader = thread.getContextClassLoader();
        thread.setContextClassLoader(IndexWriterConfig.class.getClassLoader());
        try {
            IndexWriterConfig config = new IndexWriterConfig(VERSION, analyzer);
            config.setMergeScheduler(new SerialMergeScheduler());
            if (definition.getCodec() != null) {
                config.setCodec(definition.getCodec());
            }
            return config;
        } finally {
            thread.setContextClassLoader(loader);
        }
    }

    private static Directory newIndexDirectory(NodeBuilder definition)
            throws IOException {
        String path = definition.getString(PERSISTENCE_PATH);
        if (path == null) {
            return new OakDirectory(definition.child(INDEX_DATA_CHILD_NAME), new IndexDefinition(definition));
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

    private final IndexDefinition definition;

    private final NodeBuilder definitionBuilder;

    private IndexWriter writer = null;

    private long indexedNodes;

    private final IndexUpdateCallback updateCallback;

    private boolean reindex;

    LuceneIndexEditorContext(NodeBuilder definition, Analyzer analyzer, IndexUpdateCallback updateCallback) {
        this.definitionBuilder = definition;
        this.definition = new IndexDefinition(definitionBuilder);
        this.config = getIndexWriterConfig(analyzer, this.definition);
        this.indexedNodes = 0;
        this.updateCallback = updateCallback;
    }

    boolean includeProperty(String name) {
        return definition.includeProperty(name);
    }

    boolean includePropertyType(int type){
        return definition.includePropertyType(type);
    }

    Parser getParser() {
        return parser;
    }

    IndexWriter getWriter() throws IOException {
        if (writer == null) {
            writer = new IndexWriter(newIndexDirectory(definitionBuilder), config);
        }
        return writer;
    }

    /**
     * close writer if it's not null
     */
    void closeWriter() throws IOException {
        //If reindex or fresh index and write is null on close
        //it indicates that the index is empty. In such a case trigger
        //creation of write such that an empty Lucene index state is persisted
        //in directory
        if (reindex && writer == null){
            getWriter();
        }

        if (writer != null) {
            writer.close();

            //OAK-2029 Record the last updated status so
            //as to make IndexTracker detect changes when index
            //is stored in file system
            NodeBuilder status = definitionBuilder.child(":status");
            status.setProperty("lastUpdated", ISO8601.format(Calendar.getInstance()), Type.DATE);
            status.setProperty("indexedNodes",indexedNodes);
        }
    }

    public void enableReindexMode(){
        reindex = true;
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

    /**
     * Checks if a given property should be stored in the lucene index or not
     */
    public boolean isStored(String name) {
        return definition.isStored(name);
    }

    public boolean isFullTextEnabled() {
        return definition.isFullTextEnabled();
    }

    public boolean skipTokenization(String propertyName){
        return definition.skipTokenization(propertyName);
    }

    public int getPropertyTypes() {
        return definition.getPropertyTypes();
    }

    public IndexDefinition getDefinition() {
        return definition;
    }
}
