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
package org.apache.jackrabbit.oak.composite;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import javax.jcr.query.Query;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.NodeReferenceConstants;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.ImmutableSet;

import jersey.repackaged.com.google.common.collect.Lists;

/**
 * Tests indexing and queries when using the composite node store.
 */
@RunWith(Parameterized.class)
public class CompositeNodeStoreQueryTest extends CompositeNodeStoreQueryTestBase {

    public CompositeNodeStoreQueryTest(NodeStoreKind root, NodeStoreKind mounts) {
        super(root, mounts);
    }

    @Test
    public void propertyIndex() throws Exception {
        // create an index in both the read-only and the read-write store
        NodeBuilder b;
        NodeBuilder readOnlyBuilder = readOnlyStore.getRoot().builder();
        b = createIndexDefinition(readOnlyBuilder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, false, ImmutableSet.of("foo"), null);
        b.setProperty("excludedPaths", "/jcr:system");
        NodeBuilder globalBuilder = globalStore.getRoot().builder();
        b = createIndexDefinition(globalBuilder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, false, ImmutableSet.of("foo"), null);
        b.setProperty("excludedPaths", "/jcr:system");
        EditorHook hook = new EditorHook(
                new IndexUpdateProvider(new PropertyIndexEditorProvider().with(mip)));
        readOnlyStore.merge(readOnlyBuilder, hook, CommitInfo.EMPTY);
        globalStore.merge(globalBuilder, hook, CommitInfo.EMPTY);
        root.commit();

        // add nodes in the read-only area
        NodeBuilder builder;
        builder = readOnlyStore.getRoot().builder();
        for (int i = 0; i < 3; i++) {
            builder.child("readOnly").child("node-" + i).setProperty("foo", "bar");
        }

        readOnlyStore.merge(builder, hook, CommitInfo.EMPTY);
        root.commit();

        // run a query
        // need to login again to see changes in the read-only area
        session = createRepository(store).login(null, null);
        root = session.getLatestRoot();
        qe = root.getQueryEngine();
        assertThat(
                executeQuery("explain /jcr:root//*[@foo = 'bar']", "xpath", false).toString(),
                containsString("/* property foo = bar"));
        assertEquals("[/readOnly/node-0, /readOnly/node-1, /readOnly/node-2]",
                executeQuery("/jcr:root//*[@foo = 'bar']", "xpath").toString());

        // add nodes in the read-write area
        builder = store.getRoot().builder();
        for (int i = 0; i < 3; i++) {
            builder.child("content").child("node-" + i).setProperty("foo", "bar");
        }
        store.merge(builder, hook, CommitInfo.EMPTY);
        root.commit();

        // run a query
        assertEquals("[/content/node-0, /content/node-1, /content/node-2, " +
                "/readOnly/node-0, /readOnly/node-1, /readOnly/node-2]",
                executeQuery("/jcr:root//*[@foo = 'bar']", "xpath").toString());
        assertThat(executeQuery("explain /jcr:root/content//*[@foo = 'bar']", "xpath", false).toString(),
                containsString("/* property foo = bar"));

        // remove all data
        builder = store.getRoot().builder();
        builder.child("content").remove();
        store.merge(builder, hook, CommitInfo.EMPTY);
        root.commit();

        // run a query
        assertEquals("[]",
                executeQuery("/jcr:root/content//*[@foo = 'bar']", "xpath").toString());
    }

    @Test
    @Ignore("OAK-7995")
    public void referenceIndex() throws Exception {
        // create an index in both the read-only and the read-write store
        NodeBuilder b;
        NodeBuilder readOnlyBuilder = readOnlyStore.getRoot().builder();
        b = readOnlyBuilder.child(INDEX_DEFINITIONS_NAME).child("reference");
        b.setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME);
        b.setProperty(TYPE_PROPERTY_NAME, NodeReferenceConstants.TYPE);

        NodeBuilder globalBuilder = globalStore.getRoot().builder();
        b = globalBuilder.child(INDEX_DEFINITIONS_NAME).child("reference");
        b.setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME);
        b.setProperty(TYPE_PROPERTY_NAME, NodeReferenceConstants.TYPE);

        EditorHook hook = new EditorHook(
                new IndexUpdateProvider(new ReferenceEditorProvider().with(mip)));
        readOnlyStore.merge(readOnlyBuilder, hook, CommitInfo.EMPTY);
        globalStore.merge(globalBuilder, hook, CommitInfo.EMPTY);
        root.commit();

        NodeBuilder builder;
        builder = readOnlyStore.getRoot().builder();
        for (int i = 0; i < 3; i++) {
            builder.child("readOnly").child("node-" + i).setProperty(createProperty("foo", "u1", Type.REFERENCE));
        }
        readOnlyStore.merge(builder, hook, CommitInfo.EMPTY);
        root.commit();

        builder = store.getRoot().builder();
        builder.child("a").child("x").setProperty(createProperty("foo", "u1", Type.REFERENCE));
        store.merge(builder, hook, CommitInfo.EMPTY);
        root.commit();

        // run a query
        // need to login again to see changes in the read-only area
        session = createRepository(store).login(null, null);
        root = session.getLatestRoot();
        qe = root.getQueryEngine();
        assertThat(executeQuery("explain select * from [nt:base] " +
                "where property([*], 'Reference') = cast('u1' as reference)", Query.JCR_SQL2, false).toString(),
                containsString("/* reference "));
        // expected: also /readOnly/node-0 .. 2
        assertEquals("[/a/x, /readOnly/node-0, /readOnly/node-1, /readOnly/node-2]",
                executeQuery("select [jcr:path] from [nt:base] " +
                        "where property([*], 'Reference') = cast('u1' as reference)", Query.JCR_SQL2).toString());

    }

    public void createLuceneIndex(NodeBuilder b) {
        b = b.child(INDEX_DEFINITIONS_NAME).child("lucene");
        b.setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME);
        b.setProperty(TYPE_PROPERTY_NAME, LuceneIndexConstants.TYPE_LUCENE);
        b.setProperty(FulltextIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());
        b.setProperty(IndexConstants.ASYNC_PROPERTY_NAME,
                Lists.newArrayList("async", "nrt"), Type.STRINGS);
        b.setProperty("excludedPaths", "/jcr:system");
        NodeBuilder foo = b.child(FulltextIndexConstants.INDEX_RULES)
                .child("nt:base")
                .child(FulltextIndexConstants.PROP_NODE)
                .child("asyncFoo");
        foo.setProperty(FulltextIndexConstants.PROP_NAME, "asyncFoo");
        foo.setProperty(FulltextIndexConstants.PROP_PROPERTY_INDEX, true);
    }

    @Test
    public void luceneIndex() throws Exception {
        // create an index in both the read-only and the read-write store
        NodeBuilder readOnlyBuilder = readOnlyStore.getRoot().builder();
        // add nodes in the read-only area
        for (int i = 0; i < 3; i++) {
            NodeBuilder b = readOnlyBuilder.child("readOnly").child("node-" + i);
            b.setProperty("asyncFoo", "bar");
            b.setProperty("jcr:primaryType", "nt:base", Type.NAME);
        }
        createLuceneIndex(readOnlyBuilder);

        NodeBuilder globalBuilder = globalStore.getRoot().builder();
        createLuceneIndex(globalBuilder);

        LuceneIndexEditorProvider iep = new LuceneIndexEditorProvider(indexCopier, indexTracker, null, null, mip);
        EditorHook hook = new EditorHook(
                new IndexUpdateProvider(iep, "async", false));
        readOnlyStore.merge(readOnlyBuilder, hook, CommitInfo.EMPTY);
        globalStore.merge(globalBuilder, hook, CommitInfo.EMPTY);
        root.commit();

        readOnlyStore.merge(readOnlyBuilder, hook, CommitInfo.EMPTY);
        globalStore.merge(globalBuilder, hook, CommitInfo.EMPTY);
        root.commit();
        indexTracker.update(readOnlyStore.getRoot());
        indexTracker.update(globalStore.getRoot());

        // add nodes in the read-only area

        // run a query
        // need to login again to see changes in the read-only area
        session = createRepository(store).login(null, null);
        root = session.getLatestRoot();
        qe = root.getQueryEngine();

        indexTracker.update(store.getRoot());

        assertThat(
                executeQuery("explain /jcr:root//*[@asyncFoo = 'bar']", "xpath", false).toString(),
                containsString("/* lucene:lucene(/oak:index/lucene) asyncFoo:bar"));
        assertEquals("[/readOnly/node-0, /readOnly/node-1, /readOnly/node-2]",
                executeQuery("/jcr:root//*[@asyncFoo = 'bar']", "xpath").toString());

        // add nodes in the read-write area
        NodeBuilder builder;
        builder = store.getRoot().builder();
        for (int i = 0; i < 3; i++) {
            builder.child("content").child("node-" + i).setProperty("asyncFoo", "bar");
        }
        store.merge(builder, hook, CommitInfo.EMPTY);
        root.commit();

        // run a query
        assertThat(
                executeQuery("explain /jcr:root//*[@asyncFoo = 'bar']", "xpath", false).toString(),
                containsString("/* lucene:lucene(/oak:index/lucene) asyncFoo:bar"));
        assertEquals("[/content/node-0, /content/node-1, /content/node-2, " +
                "/readOnly/node-0, /readOnly/node-1, /readOnly/node-2]",
                executeQuery("/jcr:root//*[@asyncFoo = 'bar']", "xpath").toString());

        // remove all data
        builder = store.getRoot().builder();
        builder.child("content").remove();
        store.merge(builder, hook, CommitInfo.EMPTY);
        root.commit();

        // run a query
        assertEquals("[]",
                executeQuery("/jcr:root/content//*[@asyncFoo = 'bar']", "xpath").toString());

    }

}