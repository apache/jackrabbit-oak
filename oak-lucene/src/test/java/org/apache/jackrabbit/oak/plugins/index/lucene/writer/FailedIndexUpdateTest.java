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

import org.apache.jackrabbit.guava.common.io.Closer;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.CopyOnWriteDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.lucene.store.Directory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class FailedIndexUpdateTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private Closer closer;

    private Root root;
    private AsyncIndexUpdate asyncIndexUpdate;
    private LocalDirectoryTrackingIndexCopier copier;
    private FailOnDemandValidatorProvider failOnDemandValidatorProvider;

    private static final String TEST_CONTENT_PATH = "/test";

    @Before
    public void setup() throws Exception {
        closer = Closer.create();
        createRepository();
    }

    private void createRepository() throws IOException {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        closer.register(new ExecutorCloser(executorService));
        copier = new LocalDirectoryTrackingIndexCopier(executorService, temporaryFolder.getRoot());
        FailIfDefinedEditorProvider luceneEditorProvider = new FailIfDefinedEditorProvider(copier);

        IndexEditorProvider editorProvider = new CompositeIndexEditorProvider(new NodeCounterEditorProvider(), luceneEditorProvider);

        NodeStore store = new MemoryNodeStore(INITIAL_CONTENT);

        Oak oak = new Oak(store)
                .with(new OpenSecurityProvider())
                ;
        root = oak.createRoot();

        failOnDemandValidatorProvider = new FailOnDemandValidatorProvider();
        asyncIndexUpdate = new AsyncIndexUpdate("async", store, editorProvider);
        asyncIndexUpdate.setValidatorProviders(Collections.singletonList(failOnDemandValidatorProvider));
    }

    @After
    public void after() throws IOException {
        closer.close();
    }

    @Test
    public void workingReindexDirCleanUpOnFailureOfOtherIndex() throws Exception {
        createIndex("fails", "foo", true);
        asyncIndexUpdate.run();
        assertFalse("Indexing mustn't be failing", asyncIndexUpdate.isFailing());
        copier.clearStats();

        createIndex("reindexing", "foo", false);

        root.getTree("/").addChild("test")
                .addChild("a").setProperty("foo", "bar");
        root.commit();

        asyncIndexUpdate.run();
        assertTrue("Indexing must fail", asyncIndexUpdate.isFailing());

        Set<File> reindexingDirPaths = copier.getReindexingDirPaths();
        assertEquals(1, reindexingDirPaths.size());

        File reindexingDir = reindexingDirPaths.iterator().next();
        assertFalse("Reindexing directories must get cleaned up on failure", reindexingDir.exists());

        copier.getDirs().forEach((key, value) -> assertTrue("Writer for " + key + " must be closed", value.isClosed()));
    }

    @Test
    public void workingReindexDirCleanUpOnFailureOfMerge() throws Exception {
        failOnDemandValidatorProvider.shouldFail = true;

        createIndex("reindexing", "foo", false);

        root.getTree("/").addChild("test")
                .addChild("a").setProperty("foo", "bar");
        root.commit();

        asyncIndexUpdate.run();
        assertTrue("Indexing must fail", asyncIndexUpdate.isFailing());

        Set<File> reindexingDirPaths = copier.getReindexingDirPaths();
        assertEquals(1, reindexingDirPaths.size());

        File reindexingDir = reindexingDirPaths.iterator().next();
        assertFalse("Reindexing directories must get cleaned up on failure", reindexingDir.exists());

        copier.getDirs().forEach((key, value) -> assertTrue("Writer for " + key + " must be closed", value.isClosed()));
    }

    @Test
    public void workingIndexDirDoesNotCleanUpOnFailureOfOtherIndex() throws Exception {
        createIndex("fails", "foo", true);
        createIndex("working", "foo", false);
        asyncIndexUpdate.run();

        assertFalse("Indexing mustn't be failing", asyncIndexUpdate.isFailing());

        copier.clearStats();

        root.getTree("/").addChild("test")
                .addChild("a").setProperty("foo", "bar");
        root.commit();

        asyncIndexUpdate.run();
        assertTrue("Indexing must fail", asyncIndexUpdate.isFailing());

        Set<File> reindexingDirPaths = copier.getReindexingDirPaths();
        assertEquals("No directories are reindexing", 0, reindexingDirPaths.size());

        assertEquals("Number of open directories aren't as expected", 2, copier.getDirPaths().size());

        copier.getDirPaths().forEach((key, value) -> assertTrue(key + " must not get cleaned up on failure", value.exists()));

        copier.getDirs().forEach((key, value) -> assertTrue("Writer for " + key + " must be closed", value.isClosed()));
    }

    @Test
    public void workingIndexDirDoesNotCleanUpOnFailureOfMerge() throws Exception {
        createIndex("working", "foo", false);
        asyncIndexUpdate.run();
        assertFalse("Indexing mustn't be failing", asyncIndexUpdate.isFailing());
        copier.clearStats();

        failOnDemandValidatorProvider.shouldFail = true;

        root.getTree("/").addChild("test")
                .addChild("a").setProperty("foo", "bar");
        root.commit();

        asyncIndexUpdate.run();
        assertTrue("Indexing must fail", asyncIndexUpdate.isFailing());


        Set<File> reindexingDirPaths = copier.getReindexingDirPaths();
        assertEquals("No directories are reindexing.", 0, reindexingDirPaths.size());

        assertEquals("Number of open directories aren't as expected", 1, copier.getDirPaths().size());

        copier.getDirPaths().forEach((key, value) -> assertTrue(key + " must not get cleaned up on failure", value.exists()));

        copier.getDirs().forEach((key, value) -> assertTrue("Writer for " + key + " must be closed", value.isClosed()));
    }

    private void createIndex(String idxName, String propName, boolean shouldFail) throws CommitFailedException {
        LuceneIndexDefinitionBuilder idxBuilder = new LuceneIndexDefinitionBuilder();

        idxBuilder
                .includedPaths(TEST_CONTENT_PATH)
                .indexRule("nt:base")
                .property(propName).propertyIndex();
        Tree idx = idxBuilder.build(root.getTree("/oak:index").addChild(idxName));
        idx.setProperty("shouldFail", shouldFail);

        root.commit();
    }

    static class LocalDirectoryTrackingIndexCopier extends IndexCopier {

        private final Map<String, CopyOnWriteDirectory> dirs = new HashMap<>();
        private final Map<String, File> dirPaths = new HashMap<>();
        private final Set<File> reindexingDirPaths = new HashSet<>();

        LocalDirectoryTrackingIndexCopier(Executor executor, File indexRootDir) throws IOException {
            super(executor, indexRootDir);
        }

        @Override
        public Directory wrapForWrite(LuceneIndexDefinition definition,
                                                        Directory remote,
                                                        boolean reindexMode, String dirName,
                                                        COWDirectoryTracker cowDirectoryTracker) throws IOException {
            CopyOnWriteDirectory dir = (CopyOnWriteDirectory)
                    super.wrapForWrite(definition, remote, reindexMode, dirName, cowDirectoryTracker);

            String indexPath = definition.getIndexPath();
            dirs.put(indexPath, dir);
            File dirPath = getIndexDir(definition, indexPath, dirName);
            dirPaths.put(indexPath, dirPath);
            if (reindexMode) {
                reindexingDirPaths.add(dirPath);
            }

            return dir;
        }

        void clearStats() {
            dirs.clear();
            dirPaths.clear();
            reindexingDirPaths.clear();
        }

        Map<String, CopyOnWriteDirectory> getDirs() {
            return dirs;
        }

        Map<String, File> getDirPaths() {
            return dirPaths;
        }

        Set<File> getReindexingDirPaths() {
            return reindexingDirPaths;
        }
    }

    private static class FailIfDefinedEditorProvider extends LuceneIndexEditorProvider {
        FailIfDefinedEditorProvider(IndexCopier copier) {
            super(copier);
        }

        @Override
        public Editor getIndexEditor(@NotNull String type, @NotNull NodeBuilder definition,
                                     @NotNull NodeState root,
                                     @NotNull IndexUpdateCallback callback) throws CommitFailedException {
            Editor editor = super.getIndexEditor(type, definition, root, callback);
            if (definition.getBoolean("shouldFail")) {
                editor = new FailOnLeavePathEditor(editor, TEST_CONTENT_PATH);
            }
            return editor;
        }
    }

    private static class FailOnLeavePathEditor implements Editor {
        private final Editor delegate;
        private final String failingPath;
        final String currPath;

        FailOnLeavePathEditor(Editor delegate, String failingPath) {
            this(delegate, failingPath, "", "");
        }

        private FailOnLeavePathEditor(Editor delegate, String failingPath, String parentPath, String name) {
            this.delegate = delegate != null ? delegate : new DefaultEditor();
            this.failingPath = failingPath;
            this.currPath = ("/".equals(parentPath) ? parentPath : parentPath + "/") + name;
        }

        @Override
        public void enter(NodeState before, NodeState after) throws CommitFailedException {
            delegate.enter(before, after);
        }

        @Override
        public void leave(NodeState before, NodeState after) throws CommitFailedException {
            delegate.leave(before, after); // delegate call before failing

            if (failingPath.equals(currPath)) {
                throw new CommitFailedException("index-fail", 1, null);
            }
        }

        @Override
        public void propertyAdded(PropertyState after) throws CommitFailedException {
            delegate.propertyAdded(after);
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
            delegate.propertyChanged(before, after);
        }

        @Override
        public void propertyDeleted(PropertyState before) throws CommitFailedException {
            delegate.propertyDeleted(before);
        }

        @Override
        @Nullable
        public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
            return new FailOnLeavePathEditor(delegate.childNodeAdded(name, after), failingPath, currPath, name);
        }

        @Override
        @Nullable
        public Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
            return new FailOnLeavePathEditor(delegate.childNodeChanged(name, before, after), failingPath, currPath, name);
        }

        @Override
        @Nullable
        public Editor childNodeDeleted(String name, NodeState before) throws CommitFailedException {
            return new FailOnLeavePathEditor(delegate.childNodeDeleted(name, before), failingPath, currPath, name);
        }
    }

    static class FailOnDemandValidatorProvider extends ValidatorProvider {

        boolean shouldFail;
        static final String FAILING_PATH_FRAGMENT = INDEX_DATA_CHILD_NAME;

        @Override
        protected @Nullable Validator getRootValidator(NodeState before, NodeState after, CommitInfo info) {
            return new FailOnDemandValidator(new DefaultValidator());
        }

        class FailOnDemandValidator extends FailOnLeavePathEditor implements Validator {
            final Validator delegate;

            FailOnDemandValidator(Validator delegate) {
                super(delegate, "");
                this.delegate = delegate;
            }

            private FailOnDemandValidator(Validator delegate, String parentPath, String name) {
                super(delegate, "", parentPath, name);
                this.delegate = delegate != null ? delegate : new DefaultValidator();
            }

            @Override
            public void leave(NodeState before, NodeState after) throws CommitFailedException {
                super.leave(before, after);

                if (shouldFail && currPath.contains(FAILING_PATH_FRAGMENT)) {
                    throw new CommitFailedException("validator-fail", 1, null);
                }
            }

            @Override
            @Nullable
            public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
                return new FailOnDemandValidator(delegate.childNodeAdded(name, after), currPath, name);
            }

            @Override
            @Nullable
            public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
                return new FailOnDemandValidator(delegate.childNodeChanged(name, before, after), currPath, name);
            }

            @Override
            @Nullable
            public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
                return new FailOnDemandValidator(delegate.childNodeDeleted(name, before), currPath, name);
            }
        }
    }
}
