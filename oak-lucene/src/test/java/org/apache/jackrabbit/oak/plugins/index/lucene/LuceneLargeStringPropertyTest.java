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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.CopyOnReadDirectory;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.collect.ImmutableSet.of;
import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_NODE;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class LuceneLargeStringPropertyTest extends AbstractQueryTest {
    private ExecutorService executorService = Executors.newFixedThreadPool(2);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private String corDir = null;
    private String cowDir = null;

    private LuceneIndexEditorProvider editorProvider;

    private TestUtil.OptionalEditorProvider optionalEditorProvider = new TestUtil.OptionalEditorProvider();

    private NodeStore nodeStore;

    private LuceneIndexProvider provider;

    private ResultCountingIndexProvider queryIndexProvider;

    private QueryEngineSettings queryEngineSettings = new QueryEngineSettings();

    private LogCustomizer customizer;

    @Before
    public void setup() {
        customizer = LogCustomizer.forLogger(LuceneDocumentMaker.class.getName()).filter(Level.INFO).create();
        customizer.starting();
    }

    @After
    public void after() {
        new ExecutorCloser(executorService).close();
        IndexDefinition.setDisableStoredIndexDefinition(false);
        executorService.shutdown();
        customizer.finished();
    }

    @Override
    protected void createTestIndexNode() throws Exception {
        setTraversalEnabled(false);
    }

    @Override
    protected ContentRepository createRepository() {
        IndexCopier copier = createIndexCopier();
        editorProvider = new LuceneIndexEditorProvider(copier, new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
        provider = new LuceneIndexProvider(copier);
        queryIndexProvider = new ResultCountingIndexProvider(provider);
        nodeStore = new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT);
        return new Oak(nodeStore)
                .with(new OpenSecurityProvider())
                .with(queryIndexProvider)
                .with((Observer) provider)
                .with(editorProvider)
                .with(optionalEditorProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .with(queryEngineSettings)
                .createContentRepository();
    }

    private IndexCopier createIndexCopier() {
        try {
            return new IndexCopier(executorService, temporaryFolder.getRoot()) {
                @Override
                public Directory wrapForRead(String indexPath, LuceneIndexDefinition definition,
                                             Directory remote, String dirName) throws IOException {
                    Directory ret = super.wrapForRead(indexPath, definition, remote, dirName);
                    corDir = getFSDirPath(ret);
                    return ret;
                }

                @Override
                public Directory wrapForWrite(LuceneIndexDefinition definition,
                                              Directory remote, boolean reindexMode, String dirName,
                                              COWDirectoryTracker cowDirectoryTracker) throws IOException {
                    Directory ret = super.wrapForWrite(definition, remote, reindexMode, dirName, cowDirectoryTracker);
                    cowDir = getFSDirPath(ret);
                    return ret;
                }

                private String getFSDirPath(Directory dir) {
                    if (dir instanceof CopyOnReadDirectory) {
                        dir = ((CopyOnReadDirectory) dir).getLocal();
                    }

                    dir = unwrap(dir);

                    if (dir instanceof FSDirectory) {
                        return ((FSDirectory) dir).getDirectory().getAbsolutePath();
                    }
                    return null;
                }

                private Directory unwrap(Directory dir) {
                    if (dir instanceof FilterDirectory) {
                        return unwrap(((FilterDirectory) dir).getDelegate());
                    }
                    return dir;
                }

            };
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Tree createIndex(String name, Set<String> propNames) throws CommitFailedException {
        Tree index = root.getTree("/");
        return createIndex(index, name, propNames);
    }

    public static Tree createIndex(Tree index, String name, Set<String> propNames) throws CommitFailedException {
        Tree def = index.addChild(INDEX_DEFINITIONS_NAME).addChild(name);
        def.setProperty(JcrConstants.JCR_PRIMARYTYPE,
                INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        def.setProperty(TYPE_PROPERTY_NAME, LuceneIndexConstants.TYPE_LUCENE);
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty(FulltextIndexConstants.FULL_TEXT_ENABLED, false);
        def.setProperty(PropertyStates.createProperty(FulltextIndexConstants.INCLUDE_PROPERTY_NAMES, propNames, Type.STRINGS));
        def.setProperty(LuceneIndexConstants.SAVE_DIR_LISTING, true);
        return index.getChild(INDEX_DEFINITIONS_NAME).getChild(name);
    }

    @Test
    public void truncateStringInCaseStringIsGreaterThanMaxStringPropertyLengthAndOrderbyIsConfigured() throws Exception {
        Tree idx = createIndex("test1", of("propa"));
        Tree tr = idx.addChild(PROP_NODE).addChild("propa");
        tr.setProperty("ordered", true, Type.BOOLEAN); // in case of ordered throws error that it can't index node
        tr.setProperty("analyzed", true, Type.BOOLEAN);
        idx.addChild(PROP_NODE).addChild("propa");
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        int length = LuceneDocumentMaker.STRING_PROPERTY_MAX_LENGTH + 2;
        String generatedString = RandomStringUtils.random(length, true, true);
        String aVal ="abcd pqrs" + generatedString.substring(0, length);
        String bVal = "abcd efgh " + generatedString.substring(0, length);

        test.addChild("a").setProperty("propa", aVal);
        test.addChild("b").setProperty("propa", bVal);
        root.commit();

        boolean truncationLogPresent = false;
        String failureLog = MessageFormat.format("Truncating property :dv{0} having length {1,number,#} at path:[{2}] as it is > {3,number,#}",
                "propa", aVal.length(), "/test/a", LuceneDocumentMaker.STRING_PROPERTY_MAX_LENGTH);
        for (String log : customizer.getLogs()) {
            if (log.equals(failureLog)) {
                truncationLogPresent = true;
                break;
            }
        }
        assertTrue(truncationLogPresent);
        // order of result should be first b and then a i.e. sorted on propa
        assertQuery("select [jcr:path] from [nt:base] where contains(@propa, 'abcd') order by propa", asList("/test/b", "/test/a"));
    }

}
