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
package org.apache.jackrabbit.oak.plugins.nodetype;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_FOLDER;
import static org.apache.jackrabbit.JcrConstants.MIX_REFERENCEABLE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.easymock.EasyMock.createControl;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.Optional;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

/**
 * Test for OAK-695.
 */
public class TypeEditorTest {

    @Test
    public void ignoreHidden() throws CommitFailedException {
        EditorHook hook = new EditorHook(new TypeEditorProvider());

        NodeState root = INITIAL_CONTENT;
        NodeBuilder builder = root.builder();

        NodeState before = builder.getNodeState();
        builder.child(":hidden");
        NodeState after = builder.getNodeState();
        hook.processCommit(before, after, CommitInfo.EMPTY);

        before = after;
        builder.child(":hidden").setProperty("prop", "value");
        after = builder.getNodeState();
        hook.processCommit(before, after, CommitInfo.EMPTY);

        before = after;
        builder.getChildNode(":hidden").remove();
        after = builder.getNodeState();
        hook.processCommit(before, after, CommitInfo.EMPTY);
    }

    @Test
    public void removeNonMandatoryProperty() throws CommitFailedException {
        EffectiveType effective = createControl().createMock(EffectiveType.class);
        expect(effective.isMandatoryProperty("mandatory")).andReturn(false);

        replay(effective);

        TypeEditor editor = new TypeEditor(effective);
        editor.propertyDeleted(PropertyStates.createProperty("mandatory", ""));
    }

    @Test(expected = CommitFailedException.class)
    public void removeMandatoryProperty() throws CommitFailedException {
        EffectiveType effective = createControl().createMock(EffectiveType.class);
        expect(effective.isMandatoryProperty("mandatory")).andReturn(true);
        expect(effective.getDirectTypeNames()).andReturn(Collections.emptyList());

        replay(effective);

        TypeEditor editor = new TypeEditor(effective);
        editor.propertyDeleted(PropertyStates.createProperty("mandatory", ""));
    }

    @Test
    public void removeNonMandatoryChildNode() throws CommitFailedException {
        EffectiveType effective = createControl().createMock(EffectiveType.class);
        expect(effective.isMandatoryChildNode("mandatory")).andReturn(false);

        replay(effective);

        TypeEditor editor = new TypeEditor(effective);
        editor.childNodeDeleted("mandatory", EMPTY_NODE);
    }

    @Test(expected = CommitFailedException.class)
    public void removeMandatoryChildNode() throws CommitFailedException {
        EffectiveType effective = createControl().createMock(EffectiveType.class);
        expect(effective.isMandatoryChildNode("mandatory")).andReturn(true);
        expect(effective.getDirectTypeNames()).andReturn(Collections.emptyList());

        replay(effective);

        TypeEditor editor = new TypeEditor(effective);
        editor.childNodeDeleted("mandatory", EMPTY_NODE);
    }

    @Test
    public void addNamedPropertyWithBadRequiredType() {
        EditorHook hook = new EditorHook(new TypeEditorProvider());

        NodeState root = INITIAL_CONTENT;
        NodeBuilder builder = root.builder();

        NodeState before = builder.getNodeState();

        NodeBuilder testNode = builder.child("testNode");
        testNode.setProperty(JCR_PRIMARYTYPE, NT_FOLDER, Type.NAME);
        testNode.setProperty(JCR_MIXINTYPES, ImmutableList.of("mix:title"), Type.NAMES);
        testNode.setProperty("jcr:title", true);

        try {
            hook.processCommit(before, builder.getNodeState(), CommitInfo.EMPTY);
            fail();
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
        }
    }

    @Test
    public void changeNamedPropertyToBadRequiredType() {
        EditorHook hook = new EditorHook(new TypeEditorProvider());

        NodeState root = INITIAL_CONTENT;
        NodeBuilder builder = root.builder();
        NodeBuilder testNode = builder.child("testNode");
        testNode.setProperty(JCR_PRIMARYTYPE, NT_FOLDER, Type.NAME);
        testNode.setProperty(JCR_MIXINTYPES, ImmutableList.of("mix:title"), Type.NAMES);
        testNode.setProperty("jcr:title", "title");

        NodeState before = builder.getNodeState();

        testNode.setProperty("jcr:title", true);

        try {
            hook.processCommit(before, builder.getNodeState(), CommitInfo.EMPTY);
            fail();
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
        }
    }

    @Test
    public void addMandatoryPropertyWithBadRequiredType() {
        EditorHook hook = new EditorHook(new TypeEditorProvider());

        NodeState root = INITIAL_CONTENT;
        NodeBuilder builder = root.builder();

        NodeState before = builder.getNodeState();

        NodeBuilder acl = builder.child(AccessControlConstants.REP_POLICY);
        acl.setProperty(JCR_PRIMARYTYPE, AccessControlConstants.NT_REP_ACL, Type.NAME);
        NodeBuilder ace = acl.child("first");
        ace.setProperty(JCR_PRIMARYTYPE, AccessControlConstants.NT_REP_GRANT_ACE, Type.NAME);
        ace.setProperty(AccessControlConstants.REP_PRINCIPAL_NAME, EveryonePrincipal.NAME);
        ace.setProperty(AccessControlConstants.REP_PRIVILEGES, ImmutableList.of(PrivilegeConstants.JCR_READ), Type.STRINGS);

        try {
            hook.processCommit(before, builder.getNodeState(), CommitInfo.EMPTY);
            fail();
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(55, e.getCode());
        }
    }

    @Test
    public void changeMandatoryPropertyToBadRequiredType() {
        EditorHook hook = new EditorHook(new TypeEditorProvider());

        NodeState root = INITIAL_CONTENT;
        NodeBuilder builder = root.builder();
        NodeBuilder acl = builder.child(AccessControlConstants.REP_POLICY);
        acl.setProperty(JCR_PRIMARYTYPE, AccessControlConstants.NT_REP_ACL, Type.NAME);
        NodeBuilder ace = acl.child("first");
        ace.setProperty(JCR_PRIMARYTYPE, AccessControlConstants.NT_REP_GRANT_ACE, Type.NAME);
        ace.setProperty(AccessControlConstants.REP_PRINCIPAL_NAME, EveryonePrincipal.NAME);
        ace.setProperty(AccessControlConstants.REP_PRIVILEGES, ImmutableList.of(PrivilegeConstants.JCR_READ), Type.NAMES);

        NodeState before = builder.getNodeState();

        // change to invalid type
        ace.setProperty(AccessControlConstants.REP_PRIVILEGES, ImmutableList.of(PrivilegeConstants.JCR_READ), Type.STRINGS);

        try {
            hook.processCommit(before, builder.getNodeState(), CommitInfo.EMPTY);
            fail();
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
        }
    }

    @Test
    public void requiredTypeIsUndefined() throws CommitFailedException {
        EditorHook hook = new EditorHook(new TypeEditorProvider());

        NodeState root = INITIAL_CONTENT;
        NodeBuilder builder = root.builder();

        NodeState before = builder.getNodeState();

        builder.setProperty("any", "title");
        NodeState after = builder.getNodeState();
        hook.processCommit(before, after, CommitInfo.EMPTY);

        builder.setProperty("any", 134.34, Type.DOUBLE);
        hook.processCommit(after, builder.getNodeState(), CommitInfo.EMPTY);
    }

    @Test
    public void changeNodeTypeWExtraNodes() throws CommitFailedException {
        EditorHook hook = new EditorHook(new TypeEditorProvider());

        NodeState root = INITIAL_CONTENT;
        NodeBuilder builder = root.builder();

        NodeState before = builder.getNodeState();

        builder.child("testcontent").setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME);
        builder.child("testcontent").child("unstructured_child").setProperty(JCR_PRIMARYTYPE, "nt:unstructured",
                Type.NAME);
        NodeState after = builder.getNodeState();
        root = hook.processCommit(before, after, CommitInfo.EMPTY);

        builder = root.builder();
        before = builder.getNodeState();
        builder.child("testcontent").setProperty(JCR_PRIMARYTYPE, "nt:folder", Type.NAME);
        try {
            hook.processCommit(before, builder.getNodeState(), CommitInfo.EMPTY);
            fail("should not be able to change node type due to extra nodes");
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
        }
    }

    @Test
    public void changeNodeTypeWExtraProps() throws CommitFailedException {
        EditorHook hook = new EditorHook(new TypeEditorProvider());

        NodeState root = INITIAL_CONTENT;
        NodeBuilder builder = root.builder();

        NodeState before = builder.getNodeState();

        builder.child("testcontent").setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME);
        builder.child("testcontent").setProperty("extra", "information");

        NodeState after = builder.getNodeState();
        root = hook.processCommit(before, after, CommitInfo.EMPTY);

        builder = root.builder();
        before = builder.getNodeState();
        builder.child("testcontent").setProperty(JCR_PRIMARYTYPE, "nt:folder", Type.NAME);
        try {
            hook.processCommit(before, builder.getNodeState(), CommitInfo.EMPTY);
            fail("should not be able to change node type due to extra properties");
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
        }
    }

    @Test
    public void changeNodeTypeNewBroken() throws CommitFailedException {
        EditorHook hook = new EditorHook(new TypeEditorProvider());

        NodeState root = INITIAL_CONTENT;
        NodeBuilder builder = root.builder();

        NodeState before = builder.getNodeState();
        builder.child("testcontent").setProperty(JCR_PRIMARYTYPE, "nt:folder", Type.NAME);
        builder.child("testcontent").setProperty("extra", "information");
        try {
            hook.processCommit(before, builder.getNodeState(), CommitInfo.EMPTY);
            fail("should not be able to change node type due to extra properties");
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
        }
    }

    @Test
    public void malformedUUID() throws CommitFailedException {
        EditorHook hook = new EditorHook(new TypeEditorProvider());

        NodeState root = INITIAL_CONTENT;
        NodeBuilder builder = root.builder();

        NodeState before = builder.getNodeState();
        builder.child("testcontent").setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME);
        builder.child("testcontent").setProperty("jcr:uuid", "not-a-uuid");
        NodeState after = builder.getNodeState();
        root = hook.processCommit(before, after, CommitInfo.EMPTY);

        builder = root.builder();
        before = builder.getNodeState();
        builder.child("testcontent").setProperty(JCR_MIXINTYPES, ImmutableList.of(MIX_REFERENCEABLE), Type.NAMES);
        try {
            hook.processCommit(before, builder.getNodeState(), CommitInfo.EMPTY);
            fail("should not be able to change mixin due to illegal uuid format");
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
        }
    }

    /**
     * Test that a new node can omit the jcr:primaryType property, and the TypeEditor will set the default primary type
     * if the parent node type defines one
     *
     * @throws CommitFailedException ignored
     * @see <a href="https://issues.apache.org/jira/browse/OAK-10335">OAK-10335</a>
     */
    @Test
    public void addChildNodeNotStrictDefaultPrimaryTypeIfUnspecified() throws CommitFailedException {
        // set strict to 'false' to allow a new node to omit the jcr:primaryType property, so that the TypeEditor will
        // set the default primary type based on the effective node type of its parent
        EditorHook hook = new EditorHook(new TypeEditorProvider(false));

        NodeState root = INITIAL_CONTENT;
        NodeBuilder builder = root.builder();

        // first build a policy node at path /conf/rep:policy with the explicit primary type of rep:ACL
        // the rep:ACL node type defines a default primary type of rep:GrantACE for any child node
        NodeBuilder policyBuilder = builder.child("conf")
                .setProperty(JCR_PRIMARYTYPE, "nt:folder", Type.NAME)
                .setProperty(JCR_MIXINTYPES, ImmutableList.of(AccessControlConstants.MIX_REP_ACCESS_CONTROLLABLE), Type.NAMES)
                .child("rep:policy")
                .setProperty(JCR_PRIMARYTYPE, AccessControlConstants.NT_REP_ACL, Type.NAME);

        // capture the before state from the root builder
        NodeState before = builder.getNodeState();

        // now add a child node named "allow" to the policy node. However, omit the jcr:primaryType property.
        // we expect the type editor to set the default primary type of rep:GrantACE for this new node
        policyBuilder.child("allow")
                .setProperty(AccessControlConstants.REP_PRINCIPAL_NAME, EveryonePrincipal.NAME, Type.STRING)
                .setProperty(AccessControlConstants.REP_PRIVILEGES, ImmutableList.of("jcr:read"), Type.NAMES);

        // capture the after state from the root builder
        NodeState after = hook.processCommit(before, builder.getNodeState(), CommitInfo.EMPTY);

        // verify that the after state has the expected primary types for the policy node and its child node
        NodeState policyState = after.getChildNode("conf").getChildNode("rep:policy");
        assertEquals("expect primary types: rep:policy(rep:ACL)/allow(rep:GrantACE)",
                ImmutableMap.of(
                        // rep:policy parent node
                        AccessControlConstants.NT_REP_ACL,
                        // allow child node
                        AccessControlConstants.NT_REP_GRANT_ACE),
                ImmutableMap.of(
                        // rep:policy parent node
                        Optional.ofNullable(policyState.getProperty(JCR_PRIMARYTYPE))
                                .map(state -> state.getValue(Type.NAME)).orElse(""),
                        // allow child node
                        Optional.ofNullable(policyState.getChildNode("allow").getProperty(JCR_PRIMARYTYPE))
                                .map(state -> state.getValue(Type.NAME)).orElse("")));
    }

}
