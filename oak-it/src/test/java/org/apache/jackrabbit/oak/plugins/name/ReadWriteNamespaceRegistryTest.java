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
package org.apache.jackrabbit.oak.plugins.name;

import static org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants.REP_NAMESPACES;
import static org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants.REP_NSDATA;
import static org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants.REP_PREFIXES;
import static org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants.REP_URIS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.jcr.NamespaceException;
import javax.jcr.NamespaceRegistry;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.OakBaseTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.SetUtils;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.plugins.memory.PropertyBuilder;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Test;
import org.slf4j.event.Level;

public class ReadWriteNamespaceRegistryTest extends OakBaseTest {

    public ReadWriteNamespaceRegistryTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Override
    protected ContentSession createContentSession() {
        return new Oak(store).with(new OpenSecurityProvider())
                .with(new InitialContent())
                .with(new NamespaceEditorProvider()).createContentSession();
    }

    @Test
    public void testMappings() throws Exception {
        final ContentSession session = createContentSession();
        final Root root = session.getLatestRoot();
        NamespaceRegistry r = getNamespaceRegistry(session, root);

        assertEquals("", r.getURI(""));
        assertEquals("http://www.jcp.org/jcr/1.0", r.getURI("jcr"));
        assertEquals("http://www.jcp.org/jcr/nt/1.0", r.getURI("nt"));
        assertEquals("http://www.jcp.org/jcr/mix/1.0", r.getURI("mix"));
        assertEquals("http://www.w3.org/XML/1998/namespace", r.getURI("xml"));

        assertEquals("", r.getPrefix(""));
        assertEquals("jcr", r.getPrefix("http://www.jcp.org/jcr/1.0"));
        assertEquals("nt", r.getPrefix("http://www.jcp.org/jcr/nt/1.0"));
        assertEquals("mix", r.getPrefix("http://www.jcp.org/jcr/mix/1.0"));
        assertEquals("xml", r.getPrefix("http://www.w3.org/XML/1998/namespace"));

        r.registerNamespace("p", "n");
        assertEquals(r.getURI("p"), "n");
        assertEquals(r.getPrefix("n"), "p");

        r.registerNamespace("p2", "n2");
        assertEquals(r.getURI("p2"), "n2");
        assertEquals(r.getPrefix("n2"), "p2");

        // xml namespace check
        assertTrue(SetUtils.toSet(r.getPrefixes()).contains("xml"));
        try {
            r.registerNamespace("xml", "test");
            fail("Trying to register the namespace 'xml' must throw a NamespaceException.");
        } catch (NamespaceException ex) {
            // expected
        }
    }

    @Test
    public void testInvalidNamespace() throws Exception {
        final ContentSession session = createContentSession();
        final Root root = session.getLatestRoot();
        NamespaceRegistry r = getNamespaceRegistry(session, root);

        ReadOnlyNamespaceRegistry readOnlyNamespaceRegistry = (ReadOnlyNamespaceRegistry) r;
        readOnlyNamespaceRegistry.checkConsistency();
        NamespaceRegistryModel model = readOnlyNamespaceRegistry.createNamespaceRegistryModel();

        LogCustomizer customLogs = LogCustomizer.forLogger("org.apache.jackrabbit.oak.plugins.name.ReadWriteNamespaceRegistry").enable(Level.ERROR).create();
        try {
            customLogs.starting();
            r.registerNamespace("foo", "example.com");
            r.unregisterNamespace("foo");
            List<String> myLogs = customLogs.getLogs();
            assertEquals(1, myLogs.size());
            assertTrue(myLogs.get(0).contains("Registering invalid namespace name 'example.com' for prefix 'foo', please see"));
        }
        finally {
            customLogs.finished();
        }
    }

    @Test
    public void testNamespaceRegistryModel() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ContentSession session = createContentSession();
        Root root = session.getLatestRoot();
        ReadWriteNamespaceRegistry registry = (ReadWriteNamespaceRegistry) getNamespaceRegistry(session, root);
        Tree namespaces = root.getTree("/jcr:system/rep:namespaces");
        Tree nsdata = namespaces.getChild(REP_NSDATA);
        PropertyState prefixProp = nsdata.getProperty(REP_PREFIXES);
        PropertyState namespaceProp = nsdata.getProperty(REP_URIS);

        assertTrue(registry.checkConsistency());
        NamespaceRegistryModel model = registry.createNamespaceRegistryModel();
        assertTrue(model.isConsistent());
        assertTrue(model.isFixable());

        assertEquals(0, model.getDanglingPrefixes().size());
        assertEquals(0, model.getDanglingEncodedNamespaceUris().size());
        assertEquals(0, model.getRepairedMappings().size());

        model.dump(out);
        String dump = out.toString(StandardCharsets.UTF_8);
        assertTrue(dump.contains("This namespace registry model is consistent"));
        
        // Add a registered prefix without any mapping
        PropertyBuilder<String> builder = PropertyBuilder.copy(Type.STRING, prefixProp);
        builder.addValue("foo");
        nsdata.setProperty(builder.getPropertyState());

        // Now it cannot be fixed automatically
        assertFalse(registry.checkConsistency());
        model = registry.createNamespaceRegistryModel();
        assertFalse(model.isConsistent());
        assertFalse(model.isFixable());

        assertEquals(1, model.getDanglingPrefixes().size());
        assertEquals(0, model.getDanglingEncodedNamespaceUris().size());
        assertEquals(0, model.getRepairedMappings().size());

        model.dump(out);
        dump = out.toString(StandardCharsets.UTF_8);
        assertTrue(dump.contains("This namespace registry model is inconsistent. The inconsistency can NOT be fixed automatically."));

        NamespaceRegistryModel fixedModel = model.tryRegistryRepair();
        assertFalse(fixedModel.isConsistent());
        assertFalse(fixedModel.isFixable());

        fixedModel.dump(out);
        dump = out.toString(StandardCharsets.UTF_8);
        assertTrue(dump.contains("This namespace registry model is inconsistent. The inconsistency can NOT be fixed automatically."));

        // Now add a mapping to a namespace uri, but not the reverse mapping
        namespaces.setProperty("foo", "urn:foo", Type.STRING);

        // This is inconsistent, but can be fixed automatically
        assertFalse(registry.checkConsistency());
        model = registry.createNamespaceRegistryModel();
        assertFalse(model.isConsistent());
        assertTrue(model.isFixable());

        assertEquals(0, model.getDanglingPrefixes().size());
        assertEquals(0, model.getDanglingEncodedNamespaceUris().size());
        assertEquals(1, model.getRepairedMappings().size());

        model.dump(out);
        dump = out.toString(StandardCharsets.UTF_8);
        assertTrue(dump.contains("This namespace registry model is inconsistent. The inconsistency can be fixed automatically."));

        fixedModel = model.tryRegistryRepair();
        assertTrue(fixedModel.isConsistent());
        assertTrue(fixedModel.isFixable());

        fixedModel.dump(out);
        dump = out.toString(StandardCharsets.UTF_8);
        assertTrue(dump.contains("This namespace registry model is consistent"));

        // Add a registered namespace uri without any mapping
        builder = PropertyBuilder.copy(Type.STRING, namespaceProp);
        builder.addValue("urn:bar");
        nsdata.setProperty(builder.getPropertyState());

        // Now it again cannot be fixed automatically
        assertFalse(registry.checkConsistency());
        model = registry.createNamespaceRegistryModel();
        assertFalse(model.isConsistent());
        assertFalse(model.isFixable());

        assertEquals(0, model.getDanglingPrefixes().size());
        assertEquals(1, model.getDanglingEncodedNamespaceUris().size());
        assertEquals(1, model.getRepairedMappings().size());

        fixedModel = model.tryRegistryRepair();
        assertFalse(fixedModel.isConsistent());
        assertFalse(fixedModel.isFixable());

        // Now add a reverse mapping to a prefix, but not the forward mapping
        nsdata.setProperty("urn%3Abar", "bar", Type.STRING);

        // Now it can be fixed automatically again
        assertFalse(registry.checkConsistency());
        model = registry.createNamespaceRegistryModel();
        assertFalse(model.isConsistent());
        assertTrue(model.isFixable());

        assertEquals(0, model.getDanglingPrefixes().size());
        assertEquals(0, model.getDanglingEncodedNamespaceUris().size());
        assertEquals(2, model.getRepairedMappings().size());

        fixedModel = model.tryRegistryRepair();
        assertTrue(fixedModel.isConsistent());
        assertTrue(fixedModel.isFixable());

        // Double a registered prefix
        builder = PropertyBuilder.copy(Type.STRING, prefixProp);
        builder.addValue("foo");
        nsdata.setProperty(builder.getPropertyState());

        // Can still be fixed automatically
        assertFalse(registry.checkConsistency());
        model = registry.createNamespaceRegistryModel();
        assertFalse(model.isConsistent());
        assertTrue(model.isFixable());

        assertEquals(0, model.getDanglingPrefixes().size());
        assertEquals(0, model.getDanglingEncodedNamespaceUris().size());
        assertEquals(2, model.getRepairedMappings().size());

        fixedModel = model.tryRegistryRepair();
        assertTrue(fixedModel.isConsistent());
        assertTrue(fixedModel.isFixable());

        // Double a registered namespace uri
        builder = PropertyBuilder.copy(Type.STRING, namespaceProp);
        builder.addValue("urn:bar");
        nsdata.setProperty(builder.getPropertyState());

        // Can still be fixed automatically
        assertFalse(registry.checkConsistency());
        model = registry.createNamespaceRegistryModel();
        assertFalse(model.isConsistent());
        assertTrue(model.isFixable());

        assertEquals(0, model.getDanglingPrefixes().size());
        assertEquals(0, model.getDanglingEncodedNamespaceUris().size());
        assertEquals(2, model.getRepairedMappings().size());

        fixedModel = model.tryRegistryRepair();
        assertTrue(fixedModel.isConsistent());
        assertTrue(fixedModel.isFixable());

        // Apply the fixed model
        registry.applyModel(fixedModel);
        assertTrue(registry.checkConsistency());

        assertEquals(0, fixedModel.getDanglingPrefixes().size());
        assertEquals(0, fixedModel.getDanglingEncodedNamespaceUris().size());
        assertEquals(0, fixedModel.getRepairedMappings().size());
    }

    private static NamespaceRegistry getNamespaceRegistry(ContentSession session, Root root) {
        return new ReadWriteNamespaceRegistry(root) {
            @Override
            protected Root getWriteRoot() {
                return session.getLatestRoot();
            }
            @Override
            protected void refresh() {
                root.refresh();
            }
        };
    }
}
