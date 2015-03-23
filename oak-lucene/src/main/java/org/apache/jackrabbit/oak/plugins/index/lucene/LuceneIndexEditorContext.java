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
import java.io.InputStream;
import java.net.URL;
import java.util.Calendar;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.SuggestHelper;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.ISO8601;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LuceneIndexEditorContext {

    private static final Logger log = LoggerFactory
            .getLogger(LuceneIndexEditorContext.class);

    private static IndexWriterConfig getIndexWriterConfig(IndexDefinition definition) {
        // FIXME: Hack needed to make Lucene work in an OSGi environment
        Thread thread = Thread.currentThread();
        ClassLoader loader = thread.getContextClassLoader();
        thread.setContextClassLoader(IndexWriterConfig.class.getClassLoader());
        try {
            IndexWriterConfig config = new IndexWriterConfig(VERSION, definition.getAnalyzer());
            config.setMergeScheduler(new SerialMergeScheduler());
            if (definition.getCodec() != null) {
                config.setCodec(definition.getCodec());
            }
            return config;
        } finally {
            thread.setContextClassLoader(loader);
        }
    }

    private static Directory newIndexDirectory(IndexDefinition indexDefinition, NodeBuilder definition)
            throws IOException {
        String path = definition.getString(PERSISTENCE_PATH);
        if (path == null) {
            return new OakDirectory(definition.child(INDEX_DATA_CHILD_NAME), indexDefinition);
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

    private static final Parser defaultParser = createDefaultParser();

    private final IndexDefinition definition;

    private final NodeBuilder definitionBuilder;

    private IndexWriter writer = null;

    private long indexedNodes;

    private final IndexUpdateCallback updateCallback;

    private boolean reindex;

    private Parser parser;

    /**
     * The media types supported by the parser used.
     */
    private Set<MediaType> supportedMediaTypes;

    LuceneIndexEditorContext(NodeState root, NodeBuilder definition, IndexUpdateCallback updateCallback) {
        this.definitionBuilder = definition;
        this.definition = new IndexDefinition(root, definition);
        this.config = getIndexWriterConfig(this.definition);
        this.indexedNodes = 0;
        this.updateCallback = updateCallback;
        if (this.definition.isOfOldFormat()){
            IndexDefinition.updateDefinition(definition);
        }
    }

    Parser getParser() {
        if (parser == null){
            parser = initializeTikaParser(definition);
        }
        return parser;
    }

    IndexWriter getWriter() throws IOException {
        if (writer == null) {
            writer = new IndexWriter(newIndexDirectory(definition, definitionBuilder), config);
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

            updateSuggester();

            writer.close();

            //OAK-2029 Record the last updated status so
            //as to make IndexTracker detect changes when index
            //is stored in file system
            NodeBuilder status = definitionBuilder.child(":status");
            status.setProperty("lastUpdated", ISO8601.format(Calendar.getInstance()), Type.DATE);
            status.setProperty("indexedNodes",indexedNodes);
        }
    }

    /**
     * eventually update suggest dictionary
     * @throws IOException if suggest dictionary update fails
     */
    private void updateSuggester() throws IOException {

        if (definition.isSuggestEnabled()) {

            boolean updateSuggester = false;
            NodeBuilder suggesterStatus = definitionBuilder.child(":suggesterStatus");
            if (suggesterStatus.hasProperty("lastUpdated")) {
                PropertyState suggesterLastUpdatedValue = suggesterStatus.getProperty("lastUpdated");
                Calendar suggesterLastUpdatedTime = ISO8601.parse(suggesterLastUpdatedValue.getValue(Type.DATE));
                int updateFrequency = definition.getSuggesterUpdateFrequencyMinutes();
                suggesterLastUpdatedTime.add(Calendar.MINUTE, updateFrequency);
                if (Calendar.getInstance().after(suggesterLastUpdatedTime)) {
                    updateSuggester = true;
                }
            } else {
                updateSuggester = true;
            }

            if (updateSuggester) {
                DirectoryReader reader = DirectoryReader.open(writer, false);
                try {
                    SuggestHelper.updateSuggester(reader);
                    suggesterStatus.setProperty("lastUpdated", ISO8601.format(Calendar.getInstance()), Type.DATE);
                } catch (Throwable e) {
                    log.warn("could not update suggester", e);
                } finally {
                    reader.close();
                }
            }
        }
    }

    public void enableReindexMode(){
        reindex = true;
        IndexFormatVersion version = IndexDefinition.determineVersionForFreshIndex(definitionBuilder);
        definitionBuilder.setProperty(IndexDefinition.INDEX_VERSION, version.getVersion());
    }

    public long incIndexedNodes() {
        indexedNodes++;
        return indexedNodes;
    }

    public long getIndexedNodes() {
        return indexedNodes;
    }

    public boolean isSupportedMediaType(String type) {
        if (supportedMediaTypes == null) {
            supportedMediaTypes = getParser().getSupportedTypes(null);
        }
        return supportedMediaTypes.contains(MediaType.parse(type));
    }

    void indexUpdate() throws CommitFailedException {
        updateCallback.indexUpdate();
    }

    public IndexDefinition getDefinition() {
        return definition;
    }

    private static Parser initializeTikaParser(IndexDefinition definition) {
        if (definition.hasCustomTikaConfig()){
            InputStream is = definition.getTikaConfig();
            try {
                return new AutoDetectParser(getTikaConfig(is, definition));
            } finally {
                IOUtils.closeQuietly(is);
            }
        }
        return defaultParser;
    }

    private static AutoDetectParser createDefaultParser() {
        URL configUrl = LuceneIndexEditorContext.class.getResource("tika-config.xml");
        InputStream is = null;
        if (configUrl != null) {
            try {
                is = configUrl.openStream();
                TikaConfig config = new TikaConfig(is);
                log.info("Loaded default Tika Config from classpath {}", configUrl);
                return new AutoDetectParser(config);
            } catch (Exception e) {
                log.warn("Tika configuration not available : " + configUrl, e);
            } finally {
                IOUtils.closeQuietly(is);
            }
        } else {
            log.warn("Default Tika configuration not found from {}", configUrl);
        }
        return new AutoDetectParser();
    }

    private static TikaConfig getTikaConfig(InputStream configStream, Object source){
        try {
            return new TikaConfig(configStream);
        } catch (Exception e) {
            log.warn("Tika configuration not available : "+source, e);
        }
        return TikaConfig.getDefaultConfig();
    }
}
