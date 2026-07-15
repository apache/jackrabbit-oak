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
package org.apache.jackrabbit.oak.plugins.migration;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;
import java.util.function.Consumer;

import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.plugins.migration.NodeStateCopier.builder;
import static org.apache.jackrabbit.oak.plugins.migration.NodeStateTestUtils.commit;
import static org.apache.jackrabbit.oak.plugins.migration.NodeStateTestUtils.create;
import static org.apache.jackrabbit.oak.plugins.migration.NodeStateTestUtils.createNodeStoreWithContent;
import static org.apache.jackrabbit.oak.plugins.migration.NodeStateTestUtils.expectDifference;
import static org.junit.Assert.assertEquals;

public class NodeStateCopierTest {

    private final PropertyState primaryType =
            createProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

    private final PropertyState ntFrozenNode =
            createProperty("jcr:primaryType", "nt:frozenNode", Type.NAME);

    @Test
    public void shouldCreateMissingAncestors() throws CommitFailedException, IOException {
        final NodeStore source = createPrefilledNodeStore();
        final NodeStore target = createNodeStoreWithContent();

        builder()
                .include("/a/b/c")
                .copy(source, target);

        expectDifference()
                .childNodeChanged("/a", "/a/b")
                .childNodeDeleted("/excluded", "/a/b/excluded")
                .strict()
                .verify(source.getRoot(), target.getRoot());
    }

    @Test
    public void shouldIncludeMultiplePaths() throws CommitFailedException, IOException {
        final NodeStore source = createPrefilledNodeStore();
        final NodeStore target = createNodeStoreWithContent();

        builder()
                .include("/a/b/c/d", "/a/b/c/e")
                .copy(source, target);

        expectDifference()
                .propertyDeleted("/a/b/c/f/jcr:primaryType")
                .childNodeChanged("/a", "/a/b", "/a/b/c")
                .childNodeDeleted("/excluded", "/a/b/excluded", "/a/b/c/f")
                .strict()
                .verify(source.getRoot(), target.getRoot());
    }

    @Test
    public void shouldMergeIdenticalContent() throws CommitFailedException, IOException {
        final NodeStore source = createPrefilledNodeStore();
        final NodeStore target = createPrefilledNodeStore();

        final NodeState before = target.getRoot();
        NodeStateCopier.copyNodeStore(source, target);
        final NodeState after = target.getRoot();

        expectDifference().strict().verify(before, after);
        expectDifference().strict().verify(source.getRoot(), after);
    }

    private NodeStore createPrefilledNodeStore() throws CommitFailedException, IOException {
        final NodeStore store = createNodeStoreWithContent();
        final NodeBuilder builder = store.getRoot().builder();
        create(builder, "/excluded");
        create(builder, "/a", primaryType, createProperty("name", "a"));
        create(builder, "/a/b", primaryType, createProperty("name", "b"));
        create(builder, "/a/b/excluded");
        create(builder, "/a/b/c", primaryType, createProperty("name", "c"));
        create(builder, "/a/b/c/d", primaryType);
        create(builder, "/a/b/c/e", primaryType);
        create(builder, "/a/b/c/f", primaryType);
        commit(store, builder);
        return store;
    }

    @Test
    public void shouldSkipNonMatchingIncludes() throws CommitFailedException, IOException {
        final NodeStore source = createNodeStoreWithContent();
        final NodeBuilder builder = source.getRoot().builder();
        create(builder, "/a", primaryType);
        create(builder, "/a/b", primaryType);
        create(builder, "/a/b/c", primaryType);
        commit(source, builder);

        final NodeStore target = createNodeStoreWithContent();
        builder()
                .include("/a", "/z")
                .copy(source, target);

        expectDifference()
                .strict()
                .verify(source.getRoot(), target.getRoot());
    }

    @Test
    public void shouldCopyFromMultipleSources() throws CommitFailedException, IOException {
        final NodeStore source1 = createNodeStoreWithContent(
                "/content/foo/en", "/content/foo/de");
        final NodeStore source2 = createNodeStoreWithContent(
                "/content/bar/en", "/content/bar/de", "/content/baz/en");
        final NodeStore target = createNodeStoreWithContent();

        final NodeState before = target.getRoot();
        builder()
                .include("/content/foo")
                .copy(source1, target);
        builder()
                .include("/content/bar")
                .exclude("/content/bar/de")
                .copy(source2, target);
        final NodeState after = target.getRoot();

        expectDifference()
                .strict()
                .childNodeAdded(
                        "/content",
                        "/content/foo",
                        "/content/foo/en",
                        "/content/foo/de",
                        "/content/bar",
                        "/content/bar/en"
                )
                .verify(before, after);
    }

    @Test
    public void shouldRespectMergePaths() throws CommitFailedException, IOException {
        final NodeStore source = createNodeStoreWithContent("/content/foo/en", "/content/bar/en");
        final NodeStore target = createNodeStoreWithContent("/content/foo/de");

        builder()
                .merge("/content")
                .copy(source, target);
        final NodeState after = target.getRoot();

        expectDifference()
                .strict()
                .childNodeAdded("/content/foo/de")
                .childNodeChanged("/content", "/content/foo")
                .verify(source.getRoot(), after);
    }

    @Test
    public void shouldNotDeleteMergedExcludedPaths() throws CommitFailedException, IOException {
        final NodeStore source = createNodeStoreWithContent("/content/foo/en", "/jcr:system");
        final NodeStore target = createNodeStoreWithContent("/jcr:system/jcr:versionStorage");

        final NodeState before = target.getRoot();
        builder()
                .merge("/jcr:system")
                .exclude("/jcr:system")
                .copy(source, target);
        final NodeState after = target.getRoot();

        expectDifference()
                .strict()
                .childNodeAdded("/content", "/content/foo", "/content/foo/en")
                .verify(before, after);
    }

    @Test
    public void shouldDeleteExistingNodes() throws CommitFailedException, IOException {
        final NodeStore source = createNodeStoreWithContent("/content/foo");
        final NodeStore target = createNodeStoreWithContent("/content/bar");

        final NodeState before = target.getRoot();
        builder().copy(source, target);
        final NodeState after = target.getRoot();

        expectDifference()
                .strict()
                .childNodeAdded("/content/foo")
                .childNodeChanged("/content")
                .childNodeDeleted("/content/bar")
                .verify(before, after);
    }

    @Test
    public void shouldDeleteExistingPropertyIfMissingInSource() throws CommitFailedException, IOException {
        final NodeStore source = createNodeStoreWithContent("/a");
        final NodeStore target = createNodeStoreWithContent();
        final NodeBuilder builder = target.getRoot().builder();
        create(builder, "/a", primaryType);
        commit(target, builder);

        final NodeState before = target.getRoot();
        builder().copy(source, target);
        final NodeState after = target.getRoot();

        expectDifference()
                .strict()
                .propertyDeleted("/a/jcr:primaryType")
                .childNodeChanged("/a")
                .verify(before, after);
    }

    @Test
    public void shouldNotDeleteExistingNodesIfMerged() throws CommitFailedException, IOException {
        final NodeStore source = createNodeStoreWithContent("/content/foo");
        final NodeStore target = createNodeStoreWithContent("/content/bar");

        final NodeState before = target.getRoot();
        builder()
                .merge("/content/bar")
                .copy(source, target);
        final NodeState after = target.getRoot();

        expectDifference()
                .strict()
                .childNodeAdded("/content/foo")
                .childNodeChanged("/content")
                .verify(before, after);
    }

    @Test
    public void shouldNotDeleteExistingNodesIfDescendantsOfMerged() throws CommitFailedException, IOException {
        final NodeStore source = createNodeStoreWithContent("/content/foo");
        final NodeStore target = createNodeStoreWithContent("/content/bar");

        final NodeState before = target.getRoot();
        builder()
                .merge("/content")
                .copy(source, target);
        final NodeState after = target.getRoot();

        expectDifference()
                .strict()
                .childNodeAdded("/content/foo")
                .childNodeChanged("/content")
                .verify(before, after);
    }

    @Test
    public void shouldNotDeleteExistingNodesIfPreserveOnTarget() throws CommitFailedException, IOException {
        final NodeStore source = createNodeStoreWithContent("/content/foo");
        final NodeStore target = createNodeStoreWithContent("/content/bar");

        final NodeState before = target.getRoot();
        builder()
            .preserve(true)
            .copy(source, target);
        final NodeState after = target.getRoot();

        expectDifference()
            .strict()
            .childNodeAdded("/content/foo")
            .childNodeChanged("/content")
            .verify(before, after);
    }
    
    @Test
    public void shouldIgnoreNonMatchingMergePaths() throws CommitFailedException, IOException {
        final NodeStore source = createNodeStoreWithContent("/content/foo");
        final NodeStore target = createNodeStoreWithContent("/content/bar");

        final NodeState before = target.getRoot();
        builder()
                .merge("/con")
                .copy(source, target);
        final NodeState after = target.getRoot();

        expectDifference()
                .strict()
                .childNodeAdded("/content/foo")
                .childNodeChanged("/content")
                .childNodeDeleted("/content/bar")
                .verify(before, after);
    }

    @Test
    public void shouldIgnoreUUIDOfFrozenNode() throws Exception {
        final NodeStore source = createNodeStoreWithContent("/jcr:system/jcr:versionStorage");
        final NodeStore target = createNodeStoreWithContent("/jcr:system/jcr:versionStorage");

        final NodeBuilder builder = source.getRoot().builder();
        final PropertyState uuid = createProperty(JcrConstants.JCR_UUID, UUID.randomUUID().toString());
        create(builder, "/jcr:system/jcr:versionStorage/frozen", ntFrozenNode, uuid);
        commit(source, builder);

        final NodeState before = target.getRoot();
        builder()
                .withReferenceableFrozenNodes(false)
                .copy(source, target);
        final NodeState after = target.getRoot();

        expectDifference()
                .strict()
                .childNodeChanged("/jcr:system")
                .childNodeChanged("/jcr:system/jcr:versionStorage")
                .childNodeAdded("/jcr:system/jcr:versionStorage/frozen")
                .propertyAdded("/jcr:system/jcr:versionStorage/frozen/jcr:primaryType")
                .verify(before, after);
    }

    @Test
    public void shouldIncludeUUIDOfFrozenNode() throws Exception {
        final NodeStore source = createNodeStoreWithContent("/jcr:system/jcr:versionStorage");
        final NodeStore target = createNodeStoreWithContent("/jcr:system/jcr:versionStorage");

        final NodeBuilder builder = source.getRoot().builder();
        final PropertyState uuid = createProperty(JcrConstants.JCR_UUID, UUID.randomUUID().toString());
        create(builder, "/jcr:system/jcr:versionStorage/frozen", ntFrozenNode, uuid);
        commit(source, builder);

        final NodeState before = target.getRoot();
        builder()
                .withReferenceableFrozenNodes(true)
                .copy(source, target);
        final NodeState after = target.getRoot();

        expectDifference()
                .strict()
                .childNodeChanged("/jcr:system")
                .childNodeChanged("/jcr:system/jcr:versionStorage")
                .childNodeAdded("/jcr:system/jcr:versionStorage/frozen")
                .propertyAdded("/jcr:system/jcr:versionStorage/frozen/jcr:uuid")
                .propertyAdded("/jcr:system/jcr:versionStorage/frozen/jcr:primaryType")
                .verify(before, after);
    }

    @Test
    public void checkConsumerNotified() throws CommitFailedException, IOException {
        final NodeStore source = createNodeStoreWithContent("/content/foo");
        final NodeStore target = createNodeStoreWithContent("/content/bar");

        final String[] consumerNotifiedPath = {""};
        final NodeState before = target.getRoot();
        builder()
            .preserve(true)
            .withNodeConsumer((Consumer<String>) path -> consumerNotifiedPath[0] = path)
            .copy(source, target);
        final NodeState after = target.getRoot();

        expectDifference()
            .strict()
            .childNodeAdded("/content/foo")
            .childNodeChanged("/content")
            .verify(before, after);
        assertEquals("/content/foo", consumerNotifiedPath[0]);
    }
}
