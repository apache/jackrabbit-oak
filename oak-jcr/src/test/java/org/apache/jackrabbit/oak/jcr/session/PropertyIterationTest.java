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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.jcr.PropertyIterator;
import javax.jcr.Session;

import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PropertyIterationTest extends AbstractRepositoryTest {
    private Session session;
    private NodeImpl<?> source;
    private Tree tree;
    private NodeImpl<NodeDelegate> node;

    public PropertyIterationTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setUp() throws Exception {
        session = getAdminSession();
        source = (NodeImpl<?>) session.getRootNode().addNode("test", "nt:unstructured");
        source.setProperty("title", "value");
        session.save();
        tree = spy(source.dlg.getTree());
        node = new NodeImpl<>(new NodeDelegate(source.sessionDelegate, tree), source.sessionContext);
    }

    /** Counts properties only on demand and reuses the iterator's size. */
    @Test
    public void lazySize() throws Exception {
        PropertyIterator properties = node.getProperties();
        assertTrue(properties.hasNext());
        verify(tree, never()).getPropertyCount();
        assertEquals(2, properties.getSize());
        assertEquals(2, properties.getSize());
        verify(tree).getPropertyCount();
        Set<String> names = new HashSet<>();
        while (properties.hasNext()) {
            names.add(properties.nextProperty().getName());
        }
        assertEquals(new HashSet<>(Arrays.asList("jcr:primaryType", "title")), names);
    }

    /** Checks for a readable property without counting all properties. */
    @Test
    public void hasProperties() throws Exception {
        Session reader = getAnonymousSession();
        NodeImpl<?> readable = (NodeImpl<?>) reader.getNode("/test");
        Tree readableTree = spy(readable.dlg.getTree());
        NodeImpl<NodeDelegate> visible = new NodeImpl<>(
                new NodeDelegate(readable.sessionDelegate, readableTree), readable.sessionContext);
        assertTrue(visible.hasProperties());
        verify(readableTree, never()).getPropertyCount();
        AccessControlUtils.addAccessControlEntry(session, "/test", EveryonePrincipal.getInstance(),
                new String[] {PrivilegeConstants.REP_READ_PROPERTIES}, false);
        session.save();
        reader.refresh(false);
        clearInvocations(readableTree);
        assertTrue(reader.nodeExists("/test"));
        assertFalse(visible.hasProperties());
        verify(readableTree, never()).getPropertyCount();
    }

    /** Counts matching properties once for both pattern overloads. */
    @Test
    public void patternSize() throws Exception {
        for (PropertyIterator properties : new PropertyIterator[] {
                node.getProperties("tit*"), node.getProperties(new String[] {"tit*"})}) {
            clearInvocations(tree);
            assertEquals(1, properties.getSize());
            assertEquals(1, properties.getSize());
            verify(tree).getProperties();
            assertEquals("title", properties.nextProperty().getName());
        }
    }

    /** Recounts a pattern after the session's visible content changes. */
    @Test
    public void updatedSize() throws Exception {
        for (PropertyIterator properties : new PropertyIterator[] {
                node.getProperties("tit*"), node.getProperties(new String[] {"tit*"})}) {
            source.setProperty("title", "value");
            clearInvocations(tree);
            assertEquals(1, properties.getSize());
            source.getProperty("title").remove();
            assertEquals(0, properties.getSize());
            assertEquals(0, properties.getSize());
            verify(tree, times(2)).getProperties();
        }
        session.save();
        assertFalse(session.propertyExists("/test/title"));
    }

    /** Recounts unfiltered sizes after real transient writes without consuming the iterator. */
    @Test
    public void updatedUnfilteredSize() throws Exception {
        PropertyIterator properties = node.getProperties();
        assertEquals(2, properties.getSize());
        source.setProperty("extra", "value");
        assertEquals(3, properties.getSize());
        assertEquals(3, properties.getSize());
        verify(tree, times(2)).getPropertyCount();
        assertEquals(0, properties.getPosition());
        session.save();
        assertEquals("value", session.getProperty("/test/extra").getString());
    }

    /** Reports removal of the parent when a deferred count is requested. */
    @Test(expected = IllegalStateException.class)
    public void staleSize() throws Exception {
        PropertyIterator properties = node.getProperties();
        source.remove();
        properties.getSize();
    }
}
