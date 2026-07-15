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
package org.apache.jackrabbit.oak.plugins.document.bundlor;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_FILE;
import static org.apache.jackrabbit.JcrConstants.NT_RESOURCE;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.childBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class BundlorConflictTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();
    private DocumentNodeStore store1;
    private DocumentNodeStore store2;
    private DocumentStore ds = new MemoryDocumentStore();

    @Before
    public void setUpBundlor() throws CommitFailedException {

        store1 = builderProvider
                .newBuilder()
                .setDocumentStore(ds)
                .memoryCacheSize(0)
                .setClusterId(1)
                .setAsyncDelay(0)
                .getNodeStore();

        store2 = builderProvider
                .newBuilder()
                .setDocumentStore(ds)
                .memoryCacheSize(0)
                .setClusterId(2)
                .setAsyncDelay(0)
                .getNodeStore();

        NodeBuilder builder = store1.getRoot().builder();
        NodeBuilder prevState = builder.child("oldState");

        //Old state nt:file
        createFile(prevState, "file");
        //Old state app:Asset
        createAsset(prevState, "assset");

        merge(store1, builder);

        NodeState registryState = BundledTypesRegistry.builder()
                .forType("nt:file", "jcr:content")
                .registry()
                .forType("app:Asset")
                .include("jcr:content")
                .include("jcr:content/metadata")
                .include("jcr:content/renditions")
                .include("jcr:content/renditions/**")
                .build();

        builder = store1.getRoot().builder();
        builder.child("jcr:system").child("documentstore").setChildNode("bundlor", registryState);
        merge(store1, builder);

        syncStores();
    }

    @Test
    public void simpleConflict() throws Exception {
        NodeBuilder root = store1.getRoot().builder();

        getRendBuilder(createAsset(root, "foo")).getChildNode("rend-orig").setProperty("meta", "orig");
        merge(store1, root);

        syncStores();

        NodeBuilder root1 = store1.getRoot().builder();
        getRendBuilder(root1.getChildNode("foo")).child("rend1");//create a new rendition

        NodeBuilder root2 = store2.getRoot().builder();
        getRendBuilder(root2.getChildNode("foo")).remove();//remove rendition parent

        merge(store1, root1);

        thrown.expect(CommitFailedException.class);
        merge(store2, root2);
    }

    @Test
    public void removeChangedNode() throws Exception {
        // do not perform retries on merge
        store2.setMaxBackOffMillis(0);

        NodeBuilder builder = store1.getRoot().builder();
        createFile(builder.child("test"), "book.jpg")
                .child("jcr:content").setProperty("jcr:data", "test");
        merge(store1, builder);

        syncStores();

        NodeBuilder b1 = store1.getRoot().builder();
        NodeBuilder b2 = store2.getRoot().builder();

        childBuilder(b1, "/test/book.jpg/jcr:content").setProperty("jcr:data", "modified");
        merge(store1, b1);

        childBuilder(b2, "/test/book.jpg/jcr:content").remove();
        try {
            merge(store2, b2);
            fail("must fail with CommitFailedException");
        } catch (CommitFailedException e) {
            assertThat(e.getMessage(), containsString("ConflictException"));
        }
    }

    @Test
    public void changeRemovedNode() throws Exception {
        // do not perform retries on merge
        store2.setMaxBackOffMillis(0);

        NodeBuilder builder = store1.getRoot().builder();
        createFile(builder.child("test"), "book.jpg")
                .child("jcr:content").setProperty("jcr:data", "test");
        merge(store1, builder);

        syncStores();

        NodeBuilder b1 = store1.getRoot().builder();
        NodeBuilder b2 = store2.getRoot().builder();

        childBuilder(b1, "/test/book.jpg/jcr:content").remove();
        merge(store1, b1);

        childBuilder(b2, "/test/book.jpg/jcr:content").setProperty("jcr:data", "modified");
        try {
            merge(store2, b2);
            fail("must fail with CommitFailedException");
        } catch (CommitFailedException e) {
            assertThat(e.getMessage(), containsString("ConflictException"));
        }
    }

    @Test
    public void removeRemovedNode() throws Exception {
        // do not perform retries on merge
        store2.setMaxBackOffMillis(0);

        NodeBuilder builder = store1.getRoot().builder();
        createFile(builder.child("test"), "book.jpg")
                .child("jcr:content").setProperty("jcr:data", "test");
        merge(store1, builder);

        syncStores();

        NodeBuilder b1 = store1.getRoot().builder();
        NodeBuilder b2 = store2.getRoot().builder();

        childBuilder(b1, "/test/book.jpg/jcr:content").remove();
        merge(store1, b1);

        childBuilder(b2, "/test/book.jpg/jcr:content").remove();
        try {
            merge(store2, b2);
            fail("must fail with CommitFailedException");
        } catch (CommitFailedException e) {
            assertThat(e.getMessage(), containsString("ConflictException"));
        }
    }

    @Test
    public void addChildOnRemovedNode() throws Exception {
        // do not perform retries on merge
        store2.setMaxBackOffMillis(0);

        NodeBuilder builder = store1.getRoot().builder();
        createFile(builder.child("test"), "book.jpg")
                .child("jcr:content").setProperty("jcr:data", "test");
        merge(store1, builder);

        syncStores();

        NodeBuilder b1 = store1.getRoot().builder();
        NodeBuilder b2 = store2.getRoot().builder();

        childBuilder(b1, "/test/book.jpg/jcr:content").remove();
        merge(store1, b1);

        childBuilder(b2, "/test/book.jpg/jcr:content").child("extra");
        try {
            merge(store2, b2);
            fail("must fail with CommitFailedException");
        } catch (CommitFailedException e) {
            assertThat(e.getMessage(), containsString("ConflictException"));
        }
    }

    @Test
    public void removeStaleTree() throws Exception {
        // do not perform retries on merge
        store2.setMaxBackOffMillis(0);

        NodeBuilder builder = store1.getRoot().builder();
        createFile(builder.child("test"), "book.jpg")
                .child("jcr:content").setProperty("jcr:data", "test");
        merge(store1, builder);

        syncStores();

        NodeBuilder b1 = store1.getRoot().builder();
        NodeBuilder b2 = store2.getRoot().builder();

        childBuilder(b1, "/test/book.jpg/jcr:content").child("extra");
        merge(store1, b1);

        childBuilder(b2, "/test/book.jpg/jcr:content").remove();
        try {
            merge(store2, b2);
            fail("must fail with CommitFailedException");
        } catch (CommitFailedException e) {
            assertThat(e.getMessage(), containsString("ConflictException"));
        }
    }

    private static void merge(NodeStore store,
                              NodeBuilder root)
            throws CommitFailedException {
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private NodeBuilder createFile(NodeBuilder builder, String childName) {
        NodeBuilder file = type(builder.child(childName), NT_FILE);
        type(file.child("jcr:content"), NT_RESOURCE);

        return file;
    }

    private NodeBuilder getRendBuilder(NodeBuilder assetBuilder) {
        return assetBuilder.getChildNode("jcr:content").getChildNode("renditions");
    }

    private NodeBuilder createAsset(NodeBuilder builder, String childName) {
        NodeBuilder asset = type(builder.child(childName), "app:Asset");
        NodeBuilder assetJC = asset.child("jcr:content");
        assetJC.child("metadata");
        assetJC.child("renditions").child("rend-orig");
        assetJC.child("comments");

        return asset;
    }

    private NodeBuilder type(NodeBuilder builder, String typeName) {
        builder.setProperty(JCR_PRIMARYTYPE, typeName);
        return builder;
    }

    private void syncStores() {
        store1.runBackgroundOperations();
        store2.runBackgroundOperations();
    }
}
