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

import java.util.ArrayList;
import java.util.List;

import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.PropertyType;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class PropertyIterationTest extends AbstractRepositoryTest {
    private Session session;
    private Tree tree;
    private NodeImpl<?> source;
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

    /** Uses the enumerated state without looking the property up again. */
    @Test
    public void reuseState() throws Exception {
        Property property = node.getProperties("title").nextProperty();
        assertEquals(PropertyType.STRING, property.getType());
        assertEquals("value", property.getString());
        verify(tree, never()).getProperty("title");
    }

    /** Refreshes an enumerated delegate after a subsequent session update. */
    @Test
    public void updateState() throws Exception {
        Property property = node.getProperties("title").nextProperty();
        assertEquals(PropertyType.STRING, property.getType());
        source.setProperty("title", 42L);
        assertEquals(PropertyType.LONG, property.getType());
        assertEquals(42L, property.getLong());
        verify(tree).getProperty("title");
        session.save();
        assertEquals(42L, session.getProperty("/test/title").getLong());
    }

    /** Does not seed a newly consumed delegate with a pre-update iterator snapshot. */
    @Test
    public void updateBeforeConsumption() throws Exception {
        // Model an enumeration that retains states from iterator creation rather than reading them lazily.
        List<PropertyState> snapshot = new ArrayList<>();
        source.dlg.getTree().getProperties().forEach(snapshot::add);
        doReturn(snapshot).when(tree).getProperties();
        PropertyIterator properties = node.getProperties();
        source.setProperty("title", 42L);
        Property property;
        do {
            property = properties.nextProperty();
        } while (!"title".equals(property.getName()));
        assertEquals(PropertyType.LONG, property.getType());
        assertEquals(42L, property.getLong());
        verify(tree).getProperty("title");
        session.save();
        assertEquals(42L, session.getProperty("/test/title").getLong());
    }
}
