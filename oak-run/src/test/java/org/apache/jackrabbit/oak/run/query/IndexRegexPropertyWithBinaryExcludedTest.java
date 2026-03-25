/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.run.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import javax.jcr.query.RowIterator;

import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.spi.blob.data.FileDataStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests indexing of regex property patterns with binary type exclusion.
 * Uses 17KB binaries to ensure they're stored externally in BlobStore
 * (above the default binariesInlineThreshold of ~16KB).
 */
public class IndexRegexPropertyWithBinaryExcludedTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private Repository repository;
    private FileStore fileStore;
    private Session session;
    private QueryManager queryManager;
    private BlobStore blobStore;

    @Before
    public void setup() throws Exception {
        FileDataStore fds = new FileDataStore();
        fds.setMinRecordLength(4096);
        fds.init(folder.newFolder("blobstore").getAbsolutePath());
        blobStore = new DataStoreBlobStore(fds);

        fileStore = createFileStore();
        repository = createRepository(fileStore);
        session = repository.login(new SimpleCredentials(UserConstants.DEFAULT_ADMIN_ID,
                UserConstants.DEFAULT_ADMIN_ID.toCharArray()));
        queryManager = session.getWorkspace().getQueryManager();
    }

    @After
    public void tearDown() {
        if (session != null) {
            session.logout();
        }
        if (repository instanceof JackrabbitRepository) {
            ((JackrabbitRepository) repository).shutdown();
        }
        if (fileStore != null) {
            fileStore.close();
        }
    }

    private FileStore createFileStore() throws IOException, InvalidFileStoreVersionException {
        return FileStoreBuilder.fileStoreBuilder(folder.getRoot())
                .withBlobStore(blobStore)
                .build();
    }

    private Repository createRepository(FileStore fileStore) {
        LuceneIndexProvider provider = new LuceneIndexProvider();
        LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider();

        return new Jcr(new Oak(SegmentNodeStoreBuilders.builder(fileStore).build()))
                .with((org.apache.jackrabbit.oak.spi.query.QueryIndexProvider) provider)
                .with((org.apache.jackrabbit.oak.spi.commit.Observer) provider)
                .with(editorProvider)
                .withAsyncIndexing("async", 1)
                .createRepository();
    }

    @Test
    public void indexRegexPropertyWithBinaryExcluded() throws Exception {
        // Create index definition
        Node oakIndex = session.getRootNode().getNode("oak:index");
        Node index = oakIndex.addNode("testRegexBinary", "oak:QueryIndexDefinition");
        index.setProperty("type", "lucene");
        index.setProperty("async", new String[]{"async", "nrt"}, PropertyType.STRING);
        index.setProperty("compatVersion", 2);
        index.setProperty("tags", new String[]{"abc"}, PropertyType.STRING);
        index.setProperty("selectionPolicy", "tag");
        index.setProperty("includedPaths", "/content");
        index.setProperty("queryPaths", "/content");

        // indexRules
        Node indexRules = index.addNode("indexRules", "nt:unstructured");
        Node ntBase = indexRules.addNode("nt:base", "nt:unstructured");
        ntBase.setProperty("includePropertyTypes",
                new String[]{"String", "Name"}, PropertyType.STRING);
        Node properties = ntBase.addNode("properties", "nt:unstructured");

        // regex property for child/.*
        Node childProp = properties.addNode("childProp", "nt:unstructured");
        childProp.setProperty("name", ".*");
        childProp.setProperty("propertyIndex", true);
        childProp.setProperty("isRegexp", true);
        childProp.setProperty("nodeScopeIndex", true);
        childProp.setProperty("analyzed", true);
        childProp.setProperty("useInExcerpt", true);
        childProp.setProperty("useInSpellcheck", true);

        session.save();

        // Test content
        Node content = session.getRootNode().addNode("content", "nt:unstructured");

        Node one = content.addNode("one", "nt:unstructured");
        one.setProperty("title", "test content");
        Node oneChild = one.addNode("child", "nt:unstructured");
        oneChild.setProperty("data", "searchable text");
        // A 17KB binary to ensure it's stored externally in BlobStore
        oneChild.setProperty("binary", session.getValueFactory().createBinary(
                new ByteArrayInputStream(createLargeBinaryData(17 * 1024))));

        Node two = content.addNode("two", "nt:unstructured");
        two.setProperty("title", "another test");
        Node twoChild = two.addNode("child", "nt:unstructured");
        twoChild.setProperty("data", "searchable text");
        twoChild.setProperty("binary", session.getValueFactory().createBinary(
                new ByteArrayInputStream(createLargeBinaryData(17 * 1024))));

        session.save();

        // Query
        String xpath = "/jcr:root/content//*[jcr:contains(., 'searchable')] option(index tag abc, traversal fail)";

        // Wait for async indexing with retry
        List<String> results = null;
        AssertionError lastError = null;
        for (int attempt = 0; attempt < 300; attempt++) {
            try {
                Query query = queryManager.createQuery("explain " + xpath, "xpath");
                RowIterator rowIt = query.execute().getRows();
                while (rowIt.hasNext()) {
                    assertTrue(rowIt.nextRow().toString().contains("lucene:testRegexBinary"));
                }
                query = queryManager.createQuery(xpath, "xpath");
                QueryResult result = query.execute();
                results = getResultPaths(result);

                // Verify we found both child nodes
                assertEquals("Expected 2 results", 2, results.size());
                assertTrue("Expected /content/one/child in results", results.contains("/content/one/child"));
                assertTrue("Expected /content/two/child in results", results.contains("/content/two/child"));

                // Success - exit retry loop
                return;
            } catch (AssertionError e) {
                lastError = e;
                Thread.sleep(100);
            }
        }

        if (lastError != null) {
            throw new AssertionError("Query did not return expected results after 30 attempts. Last results: " + results, lastError);
        }
    }

    /**
     * Create a large binary data array of specified size.
     * Uses a simple pattern to fill the data.
     */
    private byte[] createLargeBinaryData(int sizeInBytes) {
        byte[] data = new byte[sizeInBytes];
        for (int i = 0; i < sizeInBytes; i++) {
            data[i] = (byte) (i % 256);
        }
        return data;
    }

    private List<String> getResultPaths(QueryResult result) throws RepositoryException {
        List<String> paths = new ArrayList<>();
        RowIterator rows = result.getRows();
        while (rows.hasNext()) {
            Row row = rows.nextRow();
            paths.add(row.getPath());
        }
        return paths;
    }
}
