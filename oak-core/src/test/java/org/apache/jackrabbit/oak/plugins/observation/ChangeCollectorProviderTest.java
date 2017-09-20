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
package org.apache.jackrabbit.oak.plugins.observation;

import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.security.SecurityProviderImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.commit.SimpleCommitContext;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

public class ChangeCollectorProviderTest {

    ChangeCollectorProvider collectorProvider;
    private ContentRepository contentRepository;
    private ContentSession session;
    private Recorder recorder;
    private SecurityProviderImpl securityProvider;

    class ContentChange {
        final NodeState root;
        final CommitInfo info;

        ContentChange(NodeState root, CommitInfo info) {
            this.root = root;
            this.info = info;
        }
    }

    class Recorder implements Observer {
        List<ContentChange> changes = new LinkedList<ContentChange>();

        @Override
        public void contentChanged(@Nonnull NodeState root,@Nonnull CommitInfo info) {
            changes.add(new ContentChange(root, info));
        }

    }

    protected SecurityProvider getSecurityProvider() {
        if (securityProvider == null) {
            securityProvider = new SecurityProviderImpl(ConfigurationParameters.EMPTY);
        }
        return securityProvider;
    }

    /**
     * Checks that the actual string set provided matches the expected one. A
     * match is when all elements occur, irrespective of the order.
     */
    private void assertMatches(String msg, Set<String> actuals, String... expected) {
        if ((actuals == null || actuals.size() == 0) && expected.length != 0) {
            fail("assertion failed for '" + msg + "': expected length " + expected.length + " != actual 0."
                    + " Expected: '" + Arrays.toString(expected) + "', got: '" + actuals + "'");
        } else if (expected.length == 0 && actuals != null && actuals.size() != 0) {
            fail("assertion failed for '" + msg + "': expected length == 0, actual " + actuals.size() + "."
                    + " Expected: '" + Arrays.toString(expected) + "', got: '" + actuals + "'");
        } else if (expected.length != actuals.size()) {
            fail("assertion failed for '" + msg + "': expected length (" + expected.length + ") != actual ("
                    + actuals.size() + ")." + " Expected: '" + Arrays.toString(expected) + "', got: '" + actuals + "'");
        }
        for (String anExpected : expected) {
            if (!actuals.contains(anExpected)) {
                fail("assertion failed for '" + msg + "': expected '" + anExpected + "' not found. Got: '" + actuals
                        + "'");
            }
        }
    }

    /**
     * Assumes that the recorder got 1 call, and extracts the ChangeSet from
     * that call
     */
    private ChangeSet getSingleChangeSet() {
        assertEquals(recorder.changes.size(), 1);
        CommitContext commitContext = (CommitContext) recorder.changes.get(0).info.getInfo().get(CommitContext.NAME);
        assertNotNull(commitContext);
        ChangeSet changeSet = (ChangeSet) commitContext
                .get(ChangeCollectorProvider.COMMIT_CONTEXT_OBSERVATION_CHANGESET);
        assertNotNull(changeSet);
        return changeSet;
    }

    @Before
    public void setup() throws PrivilegedActionException, CommitFailedException {
        collectorProvider = new ChangeCollectorProvider();
        recorder = new Recorder();
        Oak oak = new Oak().with(new InitialContent()).with(collectorProvider).with(recorder)
                .with(getSecurityProvider());
        contentRepository = oak.createContentRepository();

        session = Subject.doAs(SystemSubject.INSTANCE, new PrivilegedExceptionAction<ContentSession>() {
            @Override
            public ContentSession run() throws LoginException, NoSuchWorkspaceException {
                return contentRepository.login(null, null);
            }
        });
        Root root = session.getLatestRoot();
        Tree rootTree = root.getTree("/").addChild("test");
        rootTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, "test:parentType", Type.NAME);
        Tree child1 = rootTree.addChild("child1");
        child1.setProperty("child1Prop", 1);
        child1.setProperty(JcrConstants.JCR_PRIMARYTYPE, "test:childType", Type.NAME);
        Tree grandChild1 = child1.addChild("grandChild1");
        grandChild1.setProperty("grandChild1Prop", 1);
        grandChild1.setProperty(JcrConstants.JCR_PRIMARYTYPE, "test:grandChildType", Type.NAME);
        Tree greatGrandChild1 = grandChild1.addChild("greatGrandChild1");
        greatGrandChild1.setProperty("greatGrandChild1Prop", 1);
        greatGrandChild1.setProperty(JcrConstants.JCR_PRIMARYTYPE, "test:greatGrandChildType", Type.NAME);
        Tree child2 = rootTree.addChild("child2");
        child2.setProperty("child2Prop", 1);
        child2.setProperty(JcrConstants.JCR_PRIMARYTYPE, "test:childType", Type.NAME);
        Tree grandChild2 = child2.addChild("grandChild2");
        grandChild2.setProperty("grandChild2Prop", 1);
        grandChild2.setProperty(JcrConstants.JCR_PRIMARYTYPE, "test:grandChildType", Type.NAME);
        recorder.changes.clear();
        root.commit();

        ChangeSet changeSet = getSingleChangeSet();
        assertMatches("parentPaths", changeSet.getParentPaths(), "/test/child2", "/test/child1",
                "/test/child1/grandChild1/greatGrandChild1", "/", "/test", "/test/child1/grandChild1",
                "/test/child2/grandChild2");
        assertMatches("parentNodeNames", changeSet.getParentNodeNames(), "child2", "child1", "greatGrandChild1", "test",
                "grandChild1", "grandChild2");
        assertMatches("parentNodeTypes", changeSet.getParentNodeTypes(), "test:parentType", "test:childType",
                "test:grandChildType", "test:greatGrandChildType");
        assertMatches("allNodeTypes", changeSet.getAllNodeTypes(), "test:parentType", "test:childType",
                "test:grandChildType", "test:greatGrandChildType");
        assertMatches("propertyNames", changeSet.getPropertyNames(), JcrConstants.JCR_PRIMARYTYPE, "child1Prop",
                "child2Prop", "grandChild1Prop", "grandChild2Prop", "greatGrandChild1Prop");

        // clear the recorder so that we start off empty
        recorder.changes.clear();
    }

    private static CommitInfo newCommitInfoWithCommitContext(String sessionId, String userId) {
        return new CommitInfo(sessionId, userId,
                ImmutableMap.<String, Object> builder().put(CommitContext.NAME, new SimpleCommitContext()).build());
    }

    @Test
    public void testNull() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("test");
        builder.setChildNode("a1").setChildNode("b1").setProperty("p1", 1);
        NodeState before = builder.getNodeState();

        builder = before.builder();
        builder.setChildNode("a2").setChildNode("b12").setProperty("p12", "12");
        NodeState after = builder.getNodeState();

        assertNull(collectorProvider.getRootValidator(before, after, null));
        assertNull(collectorProvider.getRootValidator(before, after, CommitInfo.EMPTY));
        assertNotNull(collectorProvider.getRootValidator(before, after,
                newCommitInfoWithCommitContext(CommitInfo.OAK_UNKNOWN, CommitInfo.OAK_UNKNOWN)));
    }

    @Test
    public void testRemoveChild() throws CommitFailedException {
        Root root = session.getLatestRoot();
        Tree rootTree = root.getTree("/test");
        assertTrue(rootTree.getChild("child1").remove());

        root.commit();
        ChangeSet changeSet = getSingleChangeSet();
        assertMatches("parentPaths", changeSet.getParentPaths(), "/test", "/test/child1", "/test/child1/grandChild1",
                "/test/child1/grandChild1/greatGrandChild1");
        assertMatches("parentNodeNames", changeSet.getParentNodeNames(), "test", "child1", "grandChild1",
                "greatGrandChild1");
        assertMatches("parentNodeTypes", changeSet.getParentNodeTypes(), "test:parentType", "test:childType",
                "test:grandChildType", "test:greatGrandChildType");
        assertMatches("allNodeTypes", changeSet.getAllNodeTypes(), "test:parentType", "test:childType",
                "test:grandChildType", "test:greatGrandChildType");
        assertMatches("propertyNames", changeSet.getPropertyNames(), JcrConstants.JCR_PRIMARYTYPE, "child1Prop",
                "grandChild1Prop", "greatGrandChild1Prop");
    }

    @Test
    public void testRemoveGreatGrandChild() throws CommitFailedException {
        Root root = session.getLatestRoot();
        Tree rootTree = root.getTree("/test");
        assertTrue(rootTree.getChild("child1").getChild("grandChild1").getChild("greatGrandChild1").remove());

        root.commit();
        ChangeSet changeSet = getSingleChangeSet();
        assertMatches("parentPaths", changeSet.getParentPaths(), "/test/child1/grandChild1/greatGrandChild1",
                "/test/child1/grandChild1");
        assertMatches("parentNodeNames", changeSet.getParentNodeNames(), "grandChild1", "greatGrandChild1");
        assertMatches("parentNodeTypes", changeSet.getParentNodeTypes(), "test:greatGrandChildType",
                "test:grandChildType");
        assertMatches("allNodeTypes", changeSet.getAllNodeTypes(), "test:parentType", "test:childType",
                "test:grandChildType", "test:greatGrandChildType");
        assertMatches("propertyNames", changeSet.getPropertyNames(), JcrConstants.JCR_PRIMARYTYPE,
                "greatGrandChild1Prop");
    }

    @Test
    public void testChangeGreatGrandChild() throws CommitFailedException {
        Root root = session.getLatestRoot();
        Tree rootTree = root.getTree("/test");
        rootTree.getChild("child1").getChild("grandChild1").getChild("greatGrandChild1")
                .setProperty("greatGrandChild1Prop", 2);

        root.commit();
        ChangeSet changeSet = getSingleChangeSet();
        assertMatches("parentPaths", changeSet.getParentPaths(), "/test/child1/grandChild1/greatGrandChild1");
        assertMatches("parentNodeNames", changeSet.getParentNodeNames(), "greatGrandChild1");
        assertMatches("parentNodeTypes", changeSet.getParentNodeTypes(), "test:greatGrandChildType");
        assertMatches("allNodeTypes", changeSet.getAllNodeTypes(), "test:parentType", "test:childType",
                "test:grandChildType", "test:greatGrandChildType");
        assertMatches("propertyNames", changeSet.getPropertyNames(), "greatGrandChild1Prop");
    }

    @Test
    public void testChangeGreatAndGrandChild() throws CommitFailedException {
        Root root = session.getLatestRoot();
        Tree rootTree = root.getTree("/test");
        rootTree.getChild("child1").getChild("grandChild1").setProperty("grandChild1Prop", 2);
        rootTree.getChild("child1").getChild("grandChild1").getChild("greatGrandChild1")
                .setProperty("greatGrandChild1Prop", 2);

        root.commit();
        ChangeSet changeSet = getSingleChangeSet();
        assertMatches("parentPaths", changeSet.getParentPaths(), "/test/child1/grandChild1",
                "/test/child1/grandChild1/greatGrandChild1");
        assertMatches("parentNodeNames", changeSet.getParentNodeNames(), "grandChild1", "greatGrandChild1");
        assertMatches("parentNodeTypes", changeSet.getParentNodeTypes(), "test:grandChildType",
                "test:greatGrandChildType");
        assertMatches("allNodeTypes", changeSet.getAllNodeTypes(), "test:parentType", "test:childType",
                "test:grandChildType", "test:greatGrandChildType");
        assertMatches("propertyNames", changeSet.getPropertyNames(), "grandChild1Prop", "greatGrandChild1Prop");
    }

    @Test
    public void testAddEmptyChild() throws CommitFailedException {
        Root root = session.getLatestRoot();
        Tree rootTree = root.getTree("/test");
        rootTree.addChild("child");

        root.commit();
        ChangeSet changeSet = getSingleChangeSet();
        assertMatches("parentPaths", changeSet.getParentPaths(), "/test");
        assertMatches("parentNodeNames", changeSet.getParentNodeNames(), "test");
        assertMatches("parentNodeTypes", changeSet.getParentNodeTypes(), "test:parentType");
        assertMatches("allNodeTypes", changeSet.getAllNodeTypes(), "test:parentType");
        assertMatches("propertyNames", changeSet.getPropertyNames());
    }

    @Test
    public void testAddEmptyGrandChild() throws CommitFailedException {
        Root root = session.getLatestRoot();
        Tree rootTree = root.getTree("/test");
        Tree child = rootTree.addChild("child");
        child.addChild("grandChild");

        root.commit();
        ChangeSet changeSet = getSingleChangeSet();
        assertMatches("parentPaths", changeSet.getParentPaths(), "/test", "/test/child");
        assertMatches("parentNodeNames", changeSet.getParentNodeNames(), "test", "child");
        assertMatches("parentNodeTypes", changeSet.getParentNodeTypes(), "test:parentType");
        assertMatches("allNodeTypes", changeSet.getAllNodeTypes(), "test:parentType");
        assertMatches("propertyNames", changeSet.getPropertyNames());
    }

    @Test
    public void testAddNonEmptyGrandChild() throws CommitFailedException {
        Root root = session.getLatestRoot();
        Tree rootTree = root.getTree("/test");
        Tree child = rootTree.addChild("child");
        child.setProperty("childProperty", 1);
        Tree grandChild = child.addChild("grandChild");
        grandChild.setProperty("grandChildProperty", 2);

        root.commit();
        ChangeSet changeSet = getSingleChangeSet();
        assertMatches("parentPaths", changeSet.getParentPaths(), "/test", "/test/child", "/test/child/grandChild");
        assertMatches("parentNodeNames", changeSet.getParentNodeNames(), "test", "child", "grandChild");
        assertMatches("parentNodeTypes", changeSet.getParentNodeTypes(), "test:parentType");
        assertMatches("allNodeTypes", changeSet.getAllNodeTypes(), "test:parentType");
        assertMatches("propertyNames", changeSet.getPropertyNames(), "childProperty", "grandChildProperty");
    }

    @Test
    public void testAddSomeChildren() throws CommitFailedException {
        Root root = session.getLatestRoot();
        Tree rootTree = root.getTree("/test");
        for (int i = 0; i < 10; i++) {
            Tree child = rootTree.addChild("x" + i);
            child.setProperty(JcrConstants.JCR_PRIMARYTYPE, "test:type" + i, Type.NAME);
            child.setProperty("foo" + i, "bar");
        }

        root.commit();
        ChangeSet changeSet = getSingleChangeSet();
        assertMatches("parentPaths", changeSet.getParentPaths(), "/test", "/test/x0", "/test/x1", "/test/x2",
                "/test/x3", "/test/x4", "/test/x5", "/test/x6", "/test/x7", "/test/x8", "/test/x9");
        assertMatches("parentNodeNames", changeSet.getParentNodeNames(), "test", "x0", "x1", "x2", "x3", "x4", "x5",
                "x6", "x7", "x8", "x9");
        assertMatches("parentNodeTypes", changeSet.getParentNodeTypes(), "test:parentType", "test:type0", "test:type1",
                "test:type2", "test:type3", "test:type4", "test:type5", "test:type6", "test:type7", "test:type8",
                "test:type9");
        assertMatches("allNodeTypes", changeSet.getAllNodeTypes(), "test:parentType", "test:type0", "test:type1",
                "test:type2", "test:type3", "test:type4", "test:type5", "test:type6", "test:type7", "test:type8",
                "test:type9");
        assertMatches("propertyNames", changeSet.getPropertyNames(), JcrConstants.JCR_PRIMARYTYPE, "foo0", "foo1",
                "foo2", "foo3", "foo4", "foo5", "foo6", "foo7", "foo8", "foo9");
    }

    @Test
    public void testAddEmptyRemoveChildren() throws CommitFailedException {
        Root root = session.getLatestRoot();
        Tree rootTree = root.getTree("/test");
        Tree child = rootTree.addChild("child");
        child.addChild("grandChild");
        assertTrue(rootTree.getChild("child2").remove());

        root.commit();
        ChangeSet changeSet = getSingleChangeSet();
        assertMatches("parentPaths", changeSet.getParentPaths(), "/test", "/test/child", "/test/child2",
                "/test/child2/grandChild2");
        assertMatches("parentNodeNames", changeSet.getParentNodeNames(), "test", "child", "child2", "grandChild2");
        assertMatches("parentNodeTypes", changeSet.getParentNodeTypes(), "test:parentType", "test:childType",
                "test:grandChildType");
        assertMatches("allNodeTypes", changeSet.getAllNodeTypes(), "test:parentType", "test:childType",
                "test:grandChildType");
        assertMatches("propertyNames", changeSet.getPropertyNames(), JcrConstants.JCR_PRIMARYTYPE, "child2Prop",
                "grandChild2Prop");
    }

    @Test
    public void testAddMaxPathDepthAll() throws CommitFailedException, PrivilegedActionException {
        for (int i = 0; i < 16; i++) {
            setup();
            doAddMaxPathDepth(i);
        }
    }

    private void doAddMaxPathDepth(int maxPathDepth) throws CommitFailedException {
        collectorProvider.setMaxPathDepth(maxPathDepth);
        Root root = session.getLatestRoot();
        Tree rootTree = root.getTree("/test");
        Tree next = rootTree;
        for (int i = 0; i < 16; i++) {
            next = next.addChild("n" + i);
            if (i % 3 != 0) {
                next.setProperty("nextProp" + i, i);
                next.setProperty(JcrConstants.JCR_PRIMARYTYPE, i % 2 == 0 ? "test:even" : "test:odd", Type.NAME);
            }
        }
        root.commit();
        ChangeSet changeSet = getSingleChangeSet();
        List<String> expectedParentPaths = new LinkedList<String>();
        if (maxPathDepth == 0) {
            expectedParentPaths.add("/");
        } else {
            expectedParentPaths.add("/test");
        }
        for (int i = 0; i < maxPathDepth - 1; i++) {
            StringBuffer path = new StringBuffer("/test");
            for (int j = 0; j < i; j++) {
                path.append("/n" + j);
            }
            expectedParentPaths.add(path.toString());
        }
        assertMatches("parentPaths-" + maxPathDepth, changeSet.getParentPaths(),
                expectedParentPaths.toArray(new String[0]));
        assertMatches("parentNodeNames-" + maxPathDepth, changeSet.getParentNodeNames(), "test", "n0", "n1", "n2", "n3",
                "n4", "n5", "n6", "n7", "n8", "n9", "n10", "n11", "n12", "n13",
                "n14"/* , "n15" */);
        assertMatches("parentNodeTypes-" + maxPathDepth, changeSet.getParentNodeTypes(), "test:parentType", "test:even",
                "test:odd");
        assertMatches("allNodeTypes-" + maxPathDepth, changeSet.getAllNodeTypes(), "test:parentType", "test:even", "test:odd");
        assertMatches("propertyNames-" + maxPathDepth, changeSet.getPropertyNames(), JcrConstants.JCR_PRIMARYTYPE,
                /* "nextProp0", */"nextProp1", "nextProp2", /* "nextProp3", */ "nextProp4",
                "nextProp5"/* , "nextProp6" */
                , "nextProp7", "nextProp8", /* "nextProp9", */"nextProp10", "nextProp11",
                /* "nextProp12", */ "nextProp13",
                "nextProp14"/* , "nextProp15" */);
    }

    @Test
    public void testAddMixin() throws Exception {
        Root root = session.getLatestRoot();
        Tree rootTree = root.getTree("/test");
        rootTree.addChild("child").setProperty(JcrConstants.JCR_MIXINTYPES, Arrays.asList("aMixin1", "aMixin2"),
                Type.NAMES);

        root.commit();
        ChangeSet changeSet = getSingleChangeSet();
        assertMatches("parentPaths", changeSet.getParentPaths(), "/test", "/test/child");
        assertMatches("parentNodeNames", changeSet.getParentNodeNames(), "test", "child");
        assertMatches("parentNodeTypes", changeSet.getParentNodeTypes(), "test:parentType", "aMixin1", "aMixin2");
        assertMatches("allNodeTypes", changeSet.getAllNodeTypes(), "test:parentType", "aMixin1", "aMixin2");
        assertMatches("propertyNames", changeSet.getPropertyNames(), JcrConstants.JCR_MIXINTYPES);
    }

    @Test
    public void testAddNodeWithProperties() throws Exception {
        Root root = session.getLatestRoot();
        Tree rootTree = root.getTree("/test");
        Tree aChild = rootTree.addChild("newchild");
        aChild.setProperty("aProp", "aValue", Type.NAME);
        aChild.setProperty(JcrConstants.JCR_PRIMARYTYPE, "aPrimaryType", Type.NAME);

        root.commit();
        ChangeSet changeSet = getSingleChangeSet();
        assertMatches("parentPaths", changeSet.getParentPaths(), "/test", "/test/newchild");
        assertMatches("parentNodeNames", changeSet.getParentNodeNames(), "test", "newchild");
        assertMatches("parentNodeTypes", changeSet.getParentNodeTypes(), "test:parentType", "aPrimaryType");
        assertMatches("allNodeTypes", changeSet.getAllNodeTypes(), "test:parentType", "aPrimaryType");
        assertMatches("propertyNames", changeSet.getPropertyNames(), JcrConstants.JCR_PRIMARYTYPE, "aProp");
    }

    @Test
    public void testPathNotOverflown() throws Exception {
        Root root = session.getLatestRoot();
        Tree rootTree = root.getTree("/test");
        Set<String> expectedParentPaths = Sets.newHashSet();
        expectedParentPaths.add("/test");
        Set<String> expectedParentNodeNames = Sets.newHashSet();
        expectedParentNodeNames.add("test");
        Set<String> expectedParentNodeTypes = Sets.newHashSet();
        expectedParentNodeTypes.add("test:parentType");
        // do maxItems-1 iterations only, as the above already adds 1 item - to
        // avoid overflowing
        for (int i = 0; i < collectorProvider.getMaxItems() - 1; i++) {
            Tree aChild = rootTree.addChild("manychildren" + i);
            aChild.setProperty("aProperty", "foo");
            aChild.setProperty(JcrConstants.JCR_PRIMARYTYPE, "aChildPrimaryType" + i, Type.NAME);
            expectedParentPaths.add("/test/manychildren" + i);
            expectedParentNodeNames.add("manychildren" + i);
            expectedParentNodeTypes.add("aChildPrimaryType" + i);
        }
        root.commit();
        ChangeSet changeSet = getSingleChangeSet();
        assertMatches("parentPaths", changeSet.getParentPaths(), expectedParentPaths.toArray(new String[0]));
        assertMatches("parentNodeNames", changeSet.getParentNodeNames(),
                expectedParentNodeNames.toArray(new String[0]));
        assertMatches("parentNodeTypes", changeSet.getParentNodeTypes(),
                expectedParentNodeTypes.toArray(new String[0]));
        assertMatches("allNodeTypes", changeSet.getAllNodeTypes(),
                expectedParentNodeTypes.toArray(new String[0]));
        assertMatches("propertyNames", changeSet.getPropertyNames(), JcrConstants.JCR_PRIMARYTYPE, "aProperty");
    }

    @Test
    public void testPathOverflown() throws Exception {
        doTestPathOverflown(0);
        for (int overflowCnt = 1; overflowCnt <= 64 * 1024; overflowCnt += overflowCnt) {
            doTestPathOverflown(overflowCnt);
        }
    }

    private void doTestPathOverflown(int overflowCnt) throws CommitFailedException, PrivilegedActionException {
        setup();
        Root root = session.getLatestRoot();
        Tree rootTree = root.getTree("/test");
        for (int i = 0; i < collectorProvider.getMaxItems() + overflowCnt; i++) {
            rootTree.addChild("manychildren" + i).setProperty("aProperty", "foo");
            ;
        }
        root.commit();
        ChangeSet changeSet = getSingleChangeSet();
        assertEquals("parentPaths", null, changeSet.getParentPaths());
        assertEquals("parentNodeNames", null, changeSet.getParentNodeNames());
        assertMatches("parentNodeTypes", changeSet.getParentNodeTypes(), "test:parentType");
        assertMatches("allNodeTypes", changeSet.getAllNodeTypes(), "test:parentType");
        assertMatches("propertyNames", changeSet.getPropertyNames(), "aProperty");
    }

    @Test
    public void testPropertyNotOverflown() throws Exception {
        Root root = session.getLatestRoot();
        Tree rootTree = root.getTree("/test");
        Tree child1 = rootTree.getChild("child1");

        Set<String> expectedPropertyNames = Sets.newHashSet();
        for (int i = 0; i < collectorProvider.getMaxItems(); i++) {
            child1.setProperty("aProperty" + i, "foo");
            expectedPropertyNames.add("aProperty" + i);
        }
        root.commit();
        ChangeSet changeSet = getSingleChangeSet();
        assertMatches("parentPaths", changeSet.getParentPaths(), "/test/child1");
        assertMatches("parentNodeNames", changeSet.getParentNodeNames(), "child1");
        assertMatches("parentNodeTypes", changeSet.getParentNodeTypes(), "test:childType");
        assertMatches("allNodeTypes", changeSet.getAllNodeTypes(), "test:parentType", "test:childType");
        assertMatches("propertyNames", changeSet.getPropertyNames(), expectedPropertyNames.toArray(new String[0]));
    }

    @Test
    public void testPropertyOverflown() throws Exception {
        for (int overflowCnt = 1; overflowCnt <= 64 * 1024; overflowCnt += overflowCnt) {
            doTestPropertyOverflown(overflowCnt);
        }
    }

    private void doTestPropertyOverflown(int overflowCnt) throws CommitFailedException, PrivilegedActionException {
        setup();
        Root root = session.getLatestRoot();
        Tree rootTree = root.getTree("/test");
        Tree child1 = rootTree.getChild("child1");

        Set<String> expectedPropertyNames = Sets.newHashSet();
        for (int i = 0; i < collectorProvider.getMaxItems() + overflowCnt; i++) {
            child1.setProperty("aProperty" + i, "foo");
            expectedPropertyNames.add("aProperty" + i);
        }
        root.commit();
        ChangeSet changeSet = getSingleChangeSet();
        assertMatches("parentPaths", changeSet.getParentPaths(), "/test/child1");
        assertMatches("parentNodeNames", changeSet.getParentNodeNames(), "child1");
        assertMatches("parentNodeTypes", changeSet.getParentNodeTypes(), "test:childType");
        assertMatches("allNodeTypes", changeSet.getAllNodeTypes(), "test:parentType", "test:childType");
        assertEquals("propertyNames", null, changeSet.getPropertyNames());
    }

    @Test
    public void testRemoveMaxPathDepthAll() throws CommitFailedException, PrivilegedActionException {
        for (int i = 0; i < 16; i++) {
            setup();
            doRemoveMaxPathDepth(i);
        }
    }

    private void doRemoveMaxPathDepth(int maxPathDepth) throws CommitFailedException {
        collectorProvider.setMaxPathDepth(maxPathDepth);
        Root root = session.getLatestRoot();
        Tree rootTree = root.getTree("/test");
        Tree next = rootTree;
        for (int i = 0; i < 16; i++) {
            next = next.addChild("n" + i);
            if (i % 3 != 0) {
                next.setProperty("nextProp" + i, i);
                next.setProperty(JcrConstants.JCR_PRIMARYTYPE, i % 2 == 0 ? "test:even" : "test:odd", Type.NAME);
            }
        }
        root.commit();

        // now do the delete
        recorder.changes.clear();
        root = session.getLatestRoot();
        rootTree = root.getTree("/test");
        next = rootTree;
        for (int i = 0; i < 15; i++) {
            next = next.getChild("n" + i);
            if (i == 14) {
                next.remove();
            }
        }
        root.commit();

        ChangeSet changeSet = getSingleChangeSet();
        Set<String> expectedParentPaths = new HashSet<String>();
        String path = "/";
        if (maxPathDepth == 1) {
            path = "/test";
        } else if (maxPathDepth > 1) {
            path = "/test";
            for (int i = 0; i < maxPathDepth - 1; i++) {
                path = concat(path, "n" + i);
            }
        }
        expectedParentPaths.add(path);
        assertMatches("parentPaths-" + maxPathDepth, changeSet.getParentPaths(),
                expectedParentPaths.toArray(new String[0]));
        assertMatches("parentNodeNames-" + maxPathDepth, changeSet.getParentNodeNames(), "n13", "n14");
        assertMatches("parentNodeTypes-" + maxPathDepth, changeSet.getParentNodeTypes(), "test:even", "test:odd");
        assertMatches("allNodeTypes-" + maxPathDepth, changeSet.getAllNodeTypes(), "test:parentType", "test:even", "test:odd");
        assertMatches("propertyNames-" + maxPathDepth, changeSet.getPropertyNames(), JcrConstants.JCR_PRIMARYTYPE,
                "nextProp14");
    }

    @Test
    public void testChangeMaxPathDepthAll() throws CommitFailedException, PrivilegedActionException {
        for (int maxPathDepth = 0; maxPathDepth < 16; maxPathDepth++) {
            for (int changeAt = 0; changeAt < 16; changeAt++) {
                setup();
                doChangeMaxPathDepth(changeAt, maxPathDepth);
            }
        }
    }

    private void doChangeMaxPathDepth(int changeAt, int maxPathDepth) throws CommitFailedException {
        collectorProvider.setMaxPathDepth(maxPathDepth);
        Root root = session.getLatestRoot();
        Tree rootTree = root.getTree("/test");
        Tree next = rootTree;
        for (int i = 0; i < 16; i++) {
            next = next.addChild("n" + i);
            if (i % 3 != 0) {
                next.setProperty("nextProp" + i, i);
                next.setProperty(JcrConstants.JCR_PRIMARYTYPE, i % 2 == 0 ? "test:even" : "test:odd", Type.NAME);
            }
        }
        root.commit();
        recorder.changes.clear();

        // now do the change
        root = session.getLatestRoot();
        rootTree = root.getTree("/test");
        next = rootTree;
        List<String> expectedParentPaths = new LinkedList<String>();
        List<String> expectedParentNodeNames = new LinkedList<String>();
        Set<String> expectedAllNodeTypes = new HashSet<String>();
        List<String> expectedParentNodeTypes = new LinkedList<String>();
        List<String> expectedPropertyNames = new LinkedList<String>();
        expectedPropertyNames.add(JcrConstants.JCR_PRIMARYTYPE);
        String parent = "/";
        if (maxPathDepth > 0) {
            parent = "/test";
        }
        expectedAllNodeTypes.add("test:parentType");
        for (int i = 0; i <= changeAt; i++) {
            String childName = "n" + i;
            next = next.getChild(childName);
            if (i < maxPathDepth - 1) {
                parent = concat(parent, childName);
            }
            final String originalNodeTypeName = i % 2 == 0 ? "test:even" : "test:odd";
            if (i % 3 != 0) {
                if (i == changeAt) {
                    expectedParentNodeTypes.add(originalNodeTypeName);
                }
                expectedAllNodeTypes.add(originalNodeTypeName);
            }
            if (i == changeAt) {
                expectedParentNodeNames.add(next.getName());
                String propertyName = "nextProp" + i;
                next.setProperty(propertyName, i + 1);
                expectedPropertyNames.add(propertyName);
                final String changedNodeTypeName = i % 2 == 0 ? "test:evenChanged" : "test:oddChanged";
                expectedParentNodeTypes.add(changedNodeTypeName);
                expectedAllNodeTypes.add(changedNodeTypeName);
                next.setProperty(JcrConstants.JCR_PRIMARYTYPE, changedNodeTypeName, Type.NAME);
            }
        }
        expectedParentPaths.add(parent);
        root.commit();

        ChangeSet changeSet = getSingleChangeSet();
        assertMatches("parentPaths-" + changeAt + "-" + maxPathDepth, changeSet.getParentPaths(),
                expectedParentPaths.toArray(new String[0]));
        assertMatches("parentNodeNames-" + changeAt + "-" + maxPathDepth, changeSet.getParentNodeNames(),
                expectedParentNodeNames.toArray(new String[0]));
        assertMatches("parentNodeTypes-" + changeAt + "-" + maxPathDepth, changeSet.getParentNodeTypes(),
                expectedParentNodeTypes.toArray(new String[0]));
        assertMatches("allNodeTypes-" + changeAt + "-" + maxPathDepth, changeSet.getAllNodeTypes(),
                expectedAllNodeTypes.toArray(new String[0]));
        assertMatches("propertyNames-" + changeAt + "-" + maxPathDepth, changeSet.getPropertyNames(),
                expectedPropertyNames.toArray(new String[0]));
    }

}