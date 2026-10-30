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

import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.jcr.ImportUUIDBehavior;
import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.cnd.CndImporter;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;
import org.apache.jackrabbit.oak.jcr.xml.ImporterImpl;
import org.apache.jackrabbit.oak.plugins.nodetype.write.ReadWriteNodeTypeManager;
import org.apache.jackrabbit.oak.spi.xml.NodeInfo;
import org.apache.jackrabbit.oak.spi.xml.PropInfo;
import org.apache.jackrabbit.oak.spi.xml.ProtectedPropertyImporter;
import org.apache.jackrabbit.oak.spi.xml.TextValue;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ImportTypeReuseTest extends AbstractRepositoryTest {
    private Session session;
    private NodeImpl<?> target;
    private SessionContext context;
    private ReadWriteNodeTypeManager types;
    private NodeInfo node;

    public ImportTypeReuseTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setUp() throws Exception {
        session = getAdminSession();
        target = (NodeImpl<?>) session.getRootNode().addNode("test", "nt:unstructured");
        session.save();
        context = spy(target.sessionContext);
        WorkspaceImpl workspace = spy(context.getWorkspace());
        types = spy(workspace.getNodeTypeManager());
        doReturn(types).when(workspace).getNodeTypeManager();
        doReturn(workspace).when(context).getWorkspace();
        node = new NodeInfo("child", "nt:unstructured", Collections.emptyList(), null);
    }

    private PropInfo property(String name, String value) {
        return property(name, value, PropertyType.STRING);
    }

    private PropInfo property(String name, String value, int type) {
        return new PropInfo(name, type, new TextValue() {
            @Override
            public String getString() {
                return value;
            }

            @Override
            public Value getValue(int targetType) throws RepositoryException {
                return session.getValueFactory().createValue(value, targetType);
            }

            @Override
            public void dispose() {
            }
        });
    }

    private ImporterImpl startImport(List<PropInfo> properties) throws RepositoryException {
        return context.getSessionDelegate().perform(new SessionOperation<ImporterImpl>("import", true) {
            @Override
            public ImporterImpl perform() throws RepositoryException {
                ImporterImpl importer = new ImporterImpl("/test", context,
                        context.getSessionDelegate().getRoot(),
                        ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW, false);
                importer.start();
                importer.startNode(node, properties);
                return importer;
            }
        });
    }

    private Node finishImport(ImporterImpl importer) throws RepositoryException {
        context.getSessionDelegate().performVoid(new SessionOperation<Void>("finishImport", true) {
            @Override
            public void performVoid() throws RepositoryException {
                importer.endNode(node);
                importer.end();
            }
        });
        session.save();
        return session.getNode("/test/child");
    }

    /** Resolves types once for ordinary properties and persists both imported values. */
    @Test
    public void ordinaryProperties() throws Exception {
        ImporterImpl importer = startImport(Arrays.asList(property("a", "first"), property("b", "second")));
        verify(types).getEffectiveNodeType(argThat((Tree tree) -> "/test/child".equals(tree.getPath())));
        Node imported = finishImport(importer);
        assertEquals("first", imported.getProperty("a").getString());
        assertEquals("second", imported.getProperty("b").getString());
    }

    /** Uses definitions introduced by a protected callback's actual mixin change. */
    @Test
    public void protectedCallback() throws Exception {
        CndImporter.registerNodeTypes(new StringReader(
                "<test='urn:import-type-reuse'>\n"
                + "[test:initial] > nt:base\n"
                + "- before (STRING)\n"
                + "- marker (STRING) protected\n"
                + "[test:updated] mixin\n"
                + "- after (LONG)\n"
                + "- last (LONG)\n"), session);
        node = new NodeInfo("child", "test:initial", Collections.emptyList(), null);
        ProtectedPropertyImporter callback = mock(ProtectedPropertyImporter.class);
        when(callback.init(any(), any(), any(), anyBoolean(), anyInt(), any(), any())).thenReturn(true);
        doAnswer(invocation -> {
            Tree parent = invocation.getArgument(0);
            parent.setProperty(JcrConstants.JCR_MIXINTYPES,
                    Collections.singletonList("test:updated"), Type.NAMES);
            return true;
        }).when(callback).handlePropInfo(any(), any(), any());
        doReturn(Collections.singletonList(callback)).when(context).getProtectedItemImporters();

        ImporterImpl importer = startImport(Arrays.asList(property("before", "first"),
                property("marker", "change types"), property("after", "42", PropertyType.LONG),
                property("last", "43", PropertyType.LONG)));
        verify(types, times(2)).getEffectiveNodeType(argThat((Tree tree) -> "/test/child".equals(tree.getPath())));
        verify(callback).propertiesCompleted(argThat(tree -> "/test/child".equals(tree.getPath())));
        Node imported = finishImport(importer);
        assertTrue(imported.isNodeType("test:updated"));
        assertEquals("first", imported.getProperty("before").getString());
        assertEquals(PropertyType.LONG, imported.getProperty("after").getType());
        assertEquals(42L, imported.getProperty("after").getLong());
        assertEquals(43L, imported.getProperty("last").getLong());
        assertFalse(imported.hasProperty("marker"));
    }

    /** Creates an empty imported node without resolving its effective type for properties. */
    @Test
    public void emptyProperties() throws Exception {
        ImporterImpl importer = startImport(Collections.emptyList());
        verify(types, never()).getEffectiveNodeType(argThat((Tree tree) -> "/test/child".equals(tree.getPath())));
        assertEquals("nt:unstructured", finishImport(importer).getPrimaryNodeType().getName());
    }

    /** Leaves an existing protected auto-created child's regular properties unchanged. */
    @Test
    public void ignoredProperties() throws Exception {
        CndImporter.registerNodeTypes(new StringReader(
                "<test='urn:import-type-reuse'>\n"
                + "[test:parent] > nt:unstructured\n"
                + "+ child (nt:unstructured) = nt:unstructured protected autocreated\n"), session);
        target.setPrimaryType("test:parent");
        session.save();
        assertTrue(target.getNode("child").getDefinition().isProtected());
        Node imported = finishImport(startImport(Arrays.asList(property("a", "first"), property("b", "second"))));
        assertFalse(imported.hasProperty("a"));
        assertFalse(imported.hasProperty("b"));
        assertEquals("nt:unstructured", imported.getPrimaryNodeType().getName());
    }

    /** Rejects a property absent from a real restrictive node type's definitions. */
    @Test
    public void missingDefinition() throws Exception {
        node = new NodeInfo("child", "nt:folder", Collections.emptyList(), null);
        ConstraintViolationException exception = assertThrows(ConstraintViolationException.class,
                () -> startImport(Collections.singletonList(property("invalid", "value"))));
        assertTrue(exception.getMessage().contains("No matching property definition"));
        assertFalse(session.propertyExists("/test/child/invalid"));
    }
}
