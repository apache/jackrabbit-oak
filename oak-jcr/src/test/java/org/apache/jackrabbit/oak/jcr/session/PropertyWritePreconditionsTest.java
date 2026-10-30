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
package org.apache.jackrabbit.oak.jcr.session;

import javax.jcr.InvalidItemStateException;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinitionTemplate;
import javax.jcr.version.OnParentVersionAction;
import javax.jcr.version.VersionException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.PropertyDelegate;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class PropertyWritePreconditionsTest extends AbstractRepositoryTest {
    private Session session;
    private NodeImpl<?> node;

    public PropertyWritePreconditionsTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setUp() throws Exception {
        session = getAdminSession();
        node = (NodeImpl<?>) session.getRootNode().addNode("test", "nt:unstructured");
        node.setProperty("p", "old");
        session.save();
    }

    /** Avoids parent-wrapper lookups while persisting scalar, multivalue, and removal writes. */
    @Test
    public void writesDoNotReadParentPrimaryType() throws Exception {
        NodeDelegate parent = spy(node.dlg);
        PropertyImpl original = (PropertyImpl) node.getProperty("p");
        PropertyDelegate delegate = spy(original.dlg);
        doReturn(parent).when(delegate).getParent();
        Property property = new PropertyImpl(delegate, node.sessionContext);
        property.setValue("new");
        session.save();
        assertEquals("new", session.getProperty("/test/p").getString());

        node.setProperty("multiple", new String[] {"old"});
        PropertyImpl multiple = (PropertyImpl) node.getProperty("multiple");
        PropertyDelegate multipleDelegate = spy(multiple.dlg);
        doReturn(parent).when(multipleDelegate).getParent();
        new PropertyImpl(multipleDelegate, node.sessionContext).setValue(new String[] {"new", "second"});
        session.save();
        Value[] values = session.getProperty("/test/multiple").getValues();
        assertArrayEquals(new String[] {"new", "second"},
                new String[] {values[0].getString(), values[1].getString()});

        property.remove();
        session.save();
        assertFalse(session.propertyExists("/test/p"));
        verify(parent, never()).getPropertyOrNull(JcrConstants.JCR_PRIMARYTYPE);
    }

    /** Rejects a held property after its parent is removed, before checking checkout state. */
    @Test
    public void removedParent() throws Exception {
        Property property = node.getProperty("p");
        node.remove();
        assertThrows(InvalidItemStateException.class, property::remove);
        session.refresh(false);
        assertEquals("old", session.getProperty("/test/p").getString());
        assertFalse(session.hasPendingChanges());
    }

    /** Resolves a property once and persists its removal through the real session. */
    @Test
    public void nodeRemovalResolvesOnce() throws Exception {
        NodeDelegate parent = spy(node.dlg);
        Node wrapped = new NodeImpl<>(parent, node.sessionContext);
        assertNotNull(wrapped.setProperty("p", (Value) null));
        verify(parent).getPropertyOrNull("p");
        session.save();
        assertFalse(session.propertyExists("/test/p"));
    }

    /** Resolves removal correctly inside a genuinely reentrant session operation. */
    @Test
    public void nestedRemoval() throws Exception {
        NodeDelegate parent = spy(node.dlg);
        Node wrapped = new NodeImpl<>(parent, node.sessionContext);
        Property removed = node.sessionDelegate.perform(new SessionOperation<Property>("outer", true) {
            @Override
            public Property perform() throws RepositoryException {
                return wrapped.setProperty("p", (Value) null);
            }
        });
        assertNotNull(removed);
        verify(parent).getPropertyOrNull("p");
        session.save();
        assertFalse(session.propertyExists("/test/p"));
    }

    /** Returns the existing invalid placeholder when removing an absent property. */
    @Test
    public void absentRemoval() throws Exception {
        NodeDelegate parent = spy(node.dlg);
        Property result = new NodeImpl<>(parent, node.sessionContext).setProperty("missing", (Value) null);
        assertNotNull(result);
        assertThrows(InvalidItemStateException.class, result::getString);
        verify(parent).getPropertyOrNull("missing");
        verify(parent).getProperty("missing");
        session.save();
        assertFalse(session.propertyExists("/test/missing"));
        assertEquals("old", session.getProperty("/test/p").getString());
    }

    /** Rejects property writes and both removal APIs beneath a checked-in ancestor. */
    @Test
    public void inheritedCheckoutIsEnforced() throws Exception {
        node.addMixin("mix:versionable");
        Node child = node.addNode("child", "nt:unstructured");
        child.setProperty("scalar", "old");
        child.setProperty("multiple", new String[] {"old"});
        session.save();
        session.getWorkspace().getVersionManager().checkin(node.getPath());
        Property scalar = child.getProperty("scalar");
        Property multiple = child.getProperty("multiple");
        assertFalse(child.isCheckedOut());
        assertThrows(VersionException.class, () -> scalar.setValue("new"));
        assertThrows(VersionException.class, () -> multiple.setValue(new String[] {"new"}));
        assertThrows(VersionException.class, scalar::remove);
        assertThrows(VersionException.class, () -> child.setProperty("scalar", (Value) null));
        assertFalse(session.hasPendingChanges());
        assertEquals("old", scalar.getString());
        assertEquals("old", multiple.getValues()[0].getString());
    }

    /** Allows OPV IGNORE property writes/removals while the containing node is checked in. */
    @Test
    public void ignoredPropertiesRemainWritable() throws Exception {
        NodeTypeManager types = session.getWorkspace().getNodeTypeManager();
        NodeTypeTemplate mixin = types.createNodeTypeTemplate();
        mixin.setName("testIgnore");
        mixin.setMixin(true);
        PropertyDefinitionTemplate scalarDefinition = types.createPropertyDefinitionTemplate();
        scalarDefinition.setName("ignored");
        scalarDefinition.setOnParentVersion(OnParentVersionAction.IGNORE);
        mixin.getPropertyDefinitionTemplates().add(scalarDefinition);
        PropertyDefinitionTemplate multipleDefinition = types.createPropertyDefinitionTemplate();
        multipleDefinition.setName("ignoredMultiple");
        multipleDefinition.setMultiple(true);
        multipleDefinition.setOnParentVersion(OnParentVersionAction.IGNORE);
        mixin.getPropertyDefinitionTemplates().add(multipleDefinition);
        types.registerNodeType(mixin, false);
        node.addMixin("testIgnore");
        node.addMixin("mix:versionable");
        node.setProperty("ignored", "old");
        node.setProperty("ignoredMultiple", new String[] {"old"});
        session.save();
        session.getWorkspace().getVersionManager().checkin(node.getPath());
        assertFalse(node.isCheckedOut());
        node.getProperty("ignored").setValue("new");
        node.getProperty("ignoredMultiple").setValue(new String[] {"new"});
        session.save();
        assertEquals("new", session.getProperty("/test/ignored").getString());
        assertEquals("new", session.getProperty("/test/ignoredMultiple").getValues()[0].getString());
        node.getProperty("ignored").remove();
        assertNotNull(node.setProperty("ignoredMultiple", (Value) null));
        session.save();
        assertFalse(session.propertyExists("/test/ignored"));
        assertFalse(session.propertyExists("/test/ignoredMultiple"));
        assertTrue(session.propertyExists("/test/p"));
    }
}
