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
package org.apache.jackrabbit.oak.plugins.migration;

import org.apache.jackrabbit.guava.common.collect.Iterables;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.RepositoryException;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.plugins.migration.FilteringNodeState.wrap;
import static org.apache.jackrabbit.oak.plugins.migration.NodeStateTestUtils.assertExists;
import static org.apache.jackrabbit.oak.plugins.migration.NodeStateTestUtils.assertMissing;
import static org.apache.jackrabbit.oak.plugins.migration.NodeStateTestUtils.create;
import static org.apache.jackrabbit.oak.plugins.migration.NodeStateTestUtils.commit;
import static org.apache.jackrabbit.oak.plugins.migration.NodeStateTestUtils.createNodeStoreWithContent;
import static org.apache.jackrabbit.oak.plugins.migration.NodeStateTestUtils.getNodeState;
import static org.apache.jackrabbit.oak.plugins.tree.TreeConstants.OAK_CHILD_ORDER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class FilteringNodeStateTest {

    private static final Set<String> DEFAULT_INCLUDES = FilteringNodeState.ALL;

    private static final Set<String> DEFAULT_EXCLUDES = FilteringNodeState.NONE;

    private NodeState rootNodeState;

    @Before
    public void setup() throws RepositoryException, CommitFailedException, IOException {
        final NodeStore nodeStore = createNodeStoreWithContent(
                "/content/foo/de",
                "/content/foo/en",
                "/content/football/en",
                "/apps/foo/install",
                "/libs/foo/install"
        );

        final PropertyState childOrder = createProperty(OAK_CHILD_ORDER, asList("foo", "football"), Type.NAMES);
        final NodeBuilder builder = nodeStore.getRoot().builder();
        create(builder, "/content", childOrder);
        final PropertyState uuid = createProperty(JcrConstants.JCR_UUID, UUID.randomUUID().toString(), Type.STRING);
        final PropertyState ntFrozenNode = createProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_FROZENNODE, Type.NAME);
        create(builder, "/" + JcrConstants.JCR_SYSTEM + "/" + JcrConstants.JCR_VERSIONSTORAGE + "/dummy/frozen", ntFrozenNode, uuid);
        commit(nodeStore, builder);

        rootNodeState = nodeStore.getRoot();
    }

    @Test
    public void shouldNotDecorateForNullArgs() {
        final NodeState decorated = wrap("/", rootNodeState, null, null, null, null, true);
        assertSame("root should be identical to decorated", rootNodeState, decorated);
    }

    @Test
    public void shouldNotDecorateForDefaultIncludes() {
        final NodeState decorated = wrap("/", rootNodeState, DEFAULT_INCLUDES, null, null, null, true);
        assertSame("root should be identical to decorated", rootNodeState, decorated);
    }

    @Test
    public void shouldNotDecorateForDefaultExcludes() {
        final NodeState decorated = wrap("/", rootNodeState, null, DEFAULT_EXCLUDES, null, null, true);
        assertSame("root should be identical to decorated", rootNodeState, decorated);
    }

    @Test
    public void shouldNotDecorateForDefaultIncludesAndExcludes() {
        final NodeState decorated = wrap("/", rootNodeState, DEFAULT_INCLUDES, DEFAULT_EXCLUDES, null, null, true);
        assertSame("root should be identical to decorated", rootNodeState, decorated);
    }

    @Test
    public void shouldNotDecorateIncludedPath() {
        final NodeState content = getNodeState(rootNodeState, "/content");
        final NodeState decorated = wrap("/content", content, Set.of("/content"), null, null, null, true);
        assertSame("content should be identical to decorated", content, decorated);
    }

    @Test
    public void shouldNotDecorateIncludedDescendants() {
        final NodeState foo = getNodeState(rootNodeState, "/content/foo");
        final NodeState decorated = wrap("/content/foo", foo, Set.of("/content"), null, null, null, true);
        assertSame("foo should be identical to decorated", foo, decorated);
    }

    @Test
    public void shouldDecorateAncestorOfExcludedDescendants() {
        final NodeState foo = getNodeState(rootNodeState, "/content/foo");
        final NodeState decorated = wrap("/content/foo", foo, Set.of("/content"), Set.of("/content/foo/de"), null, null, true);
        assertNotSame("foo should not be identical to decorated", foo, decorated);

        assertMissing(decorated, "de");
        assertExists(decorated, "en");
        assertFalse("child nodes \"de\" should not be equal",
                getNodeState(foo, "de").equals(getNodeState(decorated, "de")));

        final NodeState en = getNodeState(decorated, "en");
        assertEquals("child nodes \"en\" should be equal", getNodeState(foo, "en"), en);
        assertTrue("child node \"en\" should not be decorated", !(en instanceof FilteringNodeState));
    }

    @Test
    public void shouldHaveCorrectChildOrderProperty() throws CommitFailedException {
        final NodeState content = rootNodeState.getChildNode("content");
        final NodeState decorated = wrap("/content", content, null, Set.of("/content/foo"), null, null, true);

        assertTrue(decorated.hasProperty(OAK_CHILD_ORDER));

        { // access via getProperty()
            final PropertyState childOrder = decorated.getProperty(OAK_CHILD_ORDER);
            final Iterable<String> values = childOrder.getValue(Type.STRINGS);
            assertEquals(List.of("football"), CollectionUtils.toList(values));
        }

        { // access via getProperties()
            final Predicate<PropertyState> isChildOrderProperty = new Predicate<PropertyState>() {
                @Override
                public boolean test(PropertyState propertyState) {
                    return OAK_CHILD_ORDER.equals(propertyState.getName());
                }
            };
            final PropertyState childOrder = Iterables.find(decorated.getProperties(), isChildOrderProperty::test);
            final Iterable<String> values = childOrder.getValue(Type.STRINGS);
            assertEquals(List.of("football"), CollectionUtils.toList(values));
        }
    }

    @Test
    public void shouldDecorateExcludedNode() {
        final NodeState decoratedRoot = wrap("/", rootNodeState, Set.of("/content"), Set.of("/content/foo/de"), null, null, true);
        final NodeState de = getNodeState(rootNodeState, "/content/foo/de");
        final NodeState decorated = getNodeState(decoratedRoot, "/content/foo/de");
        assertFalse("de should not be equal to decorated", de.equals(decorated));
        assertFalse("decorated should not exist", decorated.exists());
    }

    @Test
    public void shouldDecorateImplicitlyExcludedNode() {
        final NodeState content = getNodeState(rootNodeState, "/content");
        final NodeState decorated = wrap("/content", content, Set.of("/apps"), null, null, null, true);
        assertNotSame("content should not be identical to decorated", content, decorated);
    }


    @Test
    public void shouldHideExcludedPathsViaExists() {
        final NodeState decorated = wrap("/", rootNodeState, null, Set.of("/apps", "/libs"), null, null, true);
        assertMissing(decorated, "apps");
        assertMissing(decorated, "libs/foo/install");

        assertExists(decorated, "content/foo/de");
        assertExists(decorated, "content/foo/en");
    }

    @Test
    public void shouldHideExcludedPathsViaHasChildNode() {
        final NodeState decorated = wrap("/", rootNodeState, null, Set.of("/apps", "/libs"), null, null, true);

        assertExistingHasChildNode(decorated, "content");
        assertMissingHasChildNode(decorated, "apps");
        assertMissingHasChildNode(decorated, "libs");
    }

    @Test
    public void shouldHideExcludedPathsViaGetChildNodeNames() {
        final NodeState decorated = wrap("/", rootNodeState, null, Set.of("/apps", "/libs"), null, null, true);

        assertExistingChildNodeName(decorated, "content");
        assertMissingChildNodeName(decorated, "apps");
        assertMissingChildNodeName(decorated, "libs");
    }

    @Test
    public void shouldHideMissingIncludedPathsViaExists() {
        final NodeState decorated = wrap("/", rootNodeState, Set.of("/content"), null, null, null, true);
        assertMissing(decorated, "apps");
        assertMissing(decorated, "libs/foo/install");

        assertExists(decorated, "content/foo/de");
        assertExists(decorated, "content/foo/en");
    }

    @Test
    public void shouldHideMissingIncludedPathsViaHasChildNode() {
        final NodeState decorated = wrap("/", rootNodeState, Set.of("/content"), null, null, null, true);

        assertExistingHasChildNode(decorated, "content");
        assertMissingHasChildNode(decorated, "apps");
        assertMissingHasChildNode(decorated, "libs");
    }

    @Test
    public void shouldHideMissingIncludedPathsViaGetChildNodeNames() {
        final NodeState decorated = wrap("/", rootNodeState, Set.of("/content"), null, null, null, true);

        assertExistingChildNodeName(decorated, "content");
        assertMissingChildNodeName(decorated, "apps");
        assertMissingChildNodeName(decorated, "libs");
    }

    @Test
    public void shouldGivePrecedenceForExcludesOverIncludes() {
        final NodeState conflictingRules = wrap("/", rootNodeState, Set.of("/content"), Set.of("/content"), null, null, true);
        assertMissingChildNodeName(conflictingRules, "content");

        final NodeState overlappingRules = wrap("/", rootNodeState, Set.of("/content"), Set.of("/content/foo"), null, null, true);
        assertExistingChildNodeName(overlappingRules, "content");
        assertMissingChildNodeName(overlappingRules.getChildNode("content"), "foo");


        final NodeState overlappingRules2 = wrap("/", rootNodeState, Set.of("/content/foo"), Set.of("/content"), null, null, true);
        assertMissingChildNodeName(overlappingRules2, "content");
        assertMissingChildNodeName(overlappingRules2.getChildNode("content"), "foo");

    }

    @Test
    public void shouldRespectPathBoundariesForIncludes() {
        final NodeState decorated = wrap("/", rootNodeState, Set.of("/content/foo"), null, null, null, true);

        assertExistingChildNodeName(decorated, "content");
        assertExistingChildNodeName(decorated.getChildNode("content"), "foo");
        assertMissingChildNodeName(decorated.getChildNode("content"), "football");
    }

    @Test
    public void shouldRespectPathBoundariesForExcludes() {
        final NodeState decorated = wrap("/", rootNodeState, null, Set.of("/content/foo"), null, null, true);

        assertExistingChildNodeName(decorated, "content");
        assertMissingChildNodeName(decorated.getChildNode("content"), "foo");
        assertExistingChildNodeName(decorated.getChildNode("content"), "football");
    }

    @Test
    public void shouldDelegatePropertyCount() {
        final NodeState decorated = wrap("/", rootNodeState, null, Set.of("/content/foo/de"), null, null, true);

        assertEquals(1, getNodeState(decorated, "/content").getPropertyCount());
        assertEquals(0, getNodeState(decorated, "/content/foo").getPropertyCount());
    }


    @Test
    public void shouldDelegateGetProperty() {
        final NodeState decorated = wrap("/", rootNodeState, null, Set.of("/content/foo"), null, null, true);
        final NodeState content = getNodeState(decorated, "/content");

        assertNotNull(content.getProperty(OAK_CHILD_ORDER));
        assertNull(content.getProperty("nonexisting"));
    }


    @Test
    public void shouldDelegateHasProperty() {
        final NodeState decorated = wrap("/", rootNodeState, null, Set.of("/content/foo/de"), null, null, true);

        assertTrue(getNodeState(decorated, "/content").hasProperty(OAK_CHILD_ORDER));
        assertFalse(getNodeState(decorated, "/content").hasProperty("foo"));
    }


    @Test
    public void exists() {
        final NodeState decorated = wrap("/", rootNodeState, null, Set.of("/content/foo"), null, null, true);
        assertTrue("/content should exist and be visible", getNodeState(decorated, "/content").exists());
        assertFalse("/content/foo should be hidden", getNodeState(decorated, "/content/foo").exists());
        assertFalse("/nonexisting should not exist", getNodeState(decorated, "/nonexisting").exists());
    }

    @Test
    public void withoutReferenceableFrozenNode() {
        final NodeState decorated = wrap("/", rootNodeState, null, Set.of("/content/foo"), null, null, false);
        final NodeState frozenNode = getNodeState(decorated, "/" + JcrConstants.JCR_SYSTEM + "/" + JcrConstants.JCR_VERSIONSTORAGE + "/dummy/frozen");
        assertNull(frozenNode.getProperty(JcrConstants.JCR_UUID));
    }

    @Test
    public void withReferenceableFrozenNode() {
        final NodeState decorated = wrap("/", rootNodeState, null, Set.of("/content/foo"), null, null, true);
        final NodeState frozenNode = getNodeState(decorated, "/" + JcrConstants.JCR_SYSTEM + "/" + JcrConstants.JCR_VERSIONSTORAGE + "/dummy/frozen");
        assertNotNull(frozenNode.getProperty(JcrConstants.JCR_UUID));
    }

    private void assertExistingHasChildNode(NodeState decorated, String name) {
        assertTrue("should have child \"" + name + "\"", decorated.hasChildNode(name));
    }

    private void assertMissingHasChildNode(NodeState decorated, String name) {
        assertFalse("should not have child \"" + name + "\"", decorated.hasChildNode(name));
    }

    private void assertExistingChildNodeName(NodeState decorated, String name) {
        final Iterable<String> childNodeNames = decorated.getChildNodeNames();
        assertTrue("should list child \"" + name + "\"", Iterables.contains(childNodeNames, name));
    }

    private void assertMissingChildNodeName(NodeState decorated, String name) {
        final Iterable<String> childNodeNames = decorated.getChildNodeNames();
        assertFalse("should not list child \"" + name + "\"", Iterables.contains(childNodeNames, name));
    }
}
