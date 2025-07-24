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

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyBuilder;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Objects;

import static org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants.REP_NSDATA;
import static org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants.REP_PREFIXES;
import static org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants.REP_URIS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class NamespaceRegistryTest {

    /**
     * Artificially apply inconsistencies to the namespace registry and test if the NamespaceRegistryModel
     * handles them correctly.
     * @throws Exception
     */
    @Test
    public void testNamespaceRegistryModel() throws Exception {
        try (ContentSession session = new Oak()
                .with(new OpenSecurityProvider())
                .with(new InitialContent())
                .with(new NamespaceEditorProvider())
                .createContentSession()) {
            Root root = session.getLatestRoot();
            ReadWriteNamespaceRegistry registry = new TestNamespaceRegistry(root);
            Tree namespaces = root.getTree("/jcr:system/rep:namespaces");
            Tree nsdata = namespaces.getChild(REP_NSDATA);
            PropertyState prefixProp = nsdata.getProperty(REP_PREFIXES);
            PropertyState namespaceProp = nsdata.getProperty(REP_URIS);

            // Check the initial state of the namespace registry
            assertTrue(registry.checkConsistency());
            NamespaceRegistryModel model = NamespaceRegistryModel.create(root);
            assertNotNull(model);
            assertTrue(model.isConsistent());
            assertTrue(model.isFixable());

            assertEquals(0, model.getDanglingPrefixes().size());
            assertEquals(0, model.getDanglingEncodedNamespaceUris().size());
            assertEquals(0, model.getRepairedMappings().size());

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            model.dump(out);
            String dump = out.toString(StandardCharsets.UTF_8);
            assertTrue(dump.contains("This namespace registry model is consistent"));

            // Add a registered prefix without any mapping
            PropertyBuilder<String> builder = PropertyBuilder.copy(Type.STRING, prefixProp);
            builder.addValue("foo");
            nsdata.setProperty(builder.getPropertyState());

            // Now it cannot be fixed automatically
            assertFalse(registry.checkConsistency(root));
            model = NamespaceRegistryModel.create(root);
            assertNotNull(model);
            assertFalse(model.isConsistent());
            assertFalse(model.isFixable());

            assertEquals(1, model.getDanglingPrefixes().size());
            assertEquals(0, model.getDanglingEncodedNamespaceUris().size());
            assertEquals(0, model.getRepairedMappings().size());

            assertFalse(model.isConsistent());
            out = new ByteArrayOutputStream();
            model.dump(out);
            assertFalse(model.isConsistent());
            dump = out.toString(StandardCharsets.UTF_8);
            assertFalse(model.isConsistent());
            assertTrue(dump.contains("This namespace registry model is inconsistent. The inconsistency can NOT be fixed."));
            assertFalse(model.isConsistent());

            model = model.tryRegistryRepair();
            assertFalse(model.isConsistent());
            assertFalse(model.isFixable());

            out = new ByteArrayOutputStream();
            model.dump(out);
            dump = out.toString(StandardCharsets.UTF_8);
            assertTrue(dump.contains("This namespace registry model is inconsistent. The inconsistency can NOT be fixed."));

            // Now add a mapping to a namespace uri, but not the reverse mapping
            namespaces.setProperty("foo", "urn:foo", Type.STRING);

            // This is inconsistent, but can be fixed automatically
            assertFalse(registry.checkConsistency(root));
            model = NamespaceRegistryModel.create(root);
            assertNotNull(model);
            assertFalse(model.isConsistent());
            assertTrue(model.isFixable());

            assertEquals(0, model.getDanglingPrefixes().size());
            assertEquals(0, model.getDanglingEncodedNamespaceUris().size());
            assertEquals(1, model.getRepairedMappings().size());

            out = new ByteArrayOutputStream();
            model.dump(out);
            dump = out.toString(StandardCharsets.UTF_8);
            assertTrue(dump.contains("This namespace registry model is inconsistent. The inconsistency can be fixed."));

            model = model.tryRegistryRepair();
            assertTrue(model.isConsistent());
            assertTrue(model.isFixable());

            out = new ByteArrayOutputStream();
            model.dump(out);
            dump = out.toString(StandardCharsets.UTF_8);
            assertTrue(dump.contains("This namespace registry model is consistent"));

            // Add a registered namespace uri without any mapping
            builder = PropertyBuilder.copy(Type.STRING, namespaceProp);
            builder.addValue("urn:bar");
            nsdata.setProperty(builder.getPropertyState());

            // Now it again cannot be fixed automatically
            assertFalse(registry.checkConsistency(root));
            model = NamespaceRegistryModel.create(root);
            assertNotNull(model);
            assertFalse(model.isConsistent());
            assertFalse(model.isFixable());

            assertEquals(0, model.getDanglingPrefixes().size());
            assertEquals(1, model.getDanglingEncodedNamespaceUris().size());
            assertEquals(1, model.getRepairedMappings().size());

            model = model.tryRegistryRepair();
            assertFalse(model.isConsistent());
            assertFalse(model.isFixable());

            // Now add a reverse mapping to a prefix, but not the forward mapping
            nsdata.setProperty("urn%3Abar", "bar", Type.STRING);

            // Now it can be fixed automatically again
            assertFalse(registry.checkConsistency(root));
            model = NamespaceRegistryModel.create(root);
            assertFalse(model.isConsistent());
            assertTrue(model.isFixable());

            assertEquals(0, model.getDanglingPrefixes().size());
            assertEquals(0, model.getDanglingEncodedNamespaceUris().size());
            assertEquals(2, model.getRepairedMappings().size());

            model = model.tryRegistryRepair();
            assertTrue(model.isConsistent());
            assertTrue(model.isFixable());

            // Double a registered prefix
            builder = PropertyBuilder.copy(Type.STRING, prefixProp);
            builder.addValue("foo");
            nsdata.setProperty(builder.getPropertyState());

            // Can still be fixed automatically
            assertFalse(registry.checkConsistency(root));
            model = NamespaceRegistryModel.create(root);
            assertNotNull(model);
            assertFalse(model.isConsistent());
            assertTrue(model.isFixable());

            assertEquals(0, model.getDanglingPrefixes().size());
            assertEquals(0, model.getDanglingEncodedNamespaceUris().size());
            assertEquals(2, model.getRepairedMappings().size());

            model = model.tryRegistryRepair();
            assertTrue(model.isConsistent());
            assertTrue(model.isFixable());

            // Double a registered namespace uri
            builder = PropertyBuilder.copy(Type.STRING, namespaceProp);
            builder.addValue("urn:bar");
            nsdata.setProperty(builder.getPropertyState());

            // Can still be fixed automatically
            assertFalse(registry.checkConsistency(root));
            model = NamespaceRegistryModel.create(root);
            assertFalse(model.isConsistent());
            assertTrue(model.isFixable());

            assertEquals(0, model.getDanglingPrefixes().size());
            assertEquals(0, model.getDanglingEncodedNamespaceUris().size());
            assertEquals(2, model.getRepairedMappings().size());

            // remap a prefix
            model = model.setMappings(Collections.singletonMap("foo", "urn:foo2"));
            assertFalse(model.isConsistent());
            assertTrue(model.isFixable());

            // Add a registered namespace uri without any mapping
            builder = PropertyBuilder.copy(Type.STRING, namespaceProp);
            builder.addValue("urn:bar2");
            nsdata.setProperty(builder.getPropertyState());

            // Cannot be fixed automatically
            assertFalse(registry.checkConsistency(root));
            model = NamespaceRegistryModel.create(root);
            assertNotNull(model);
            assertFalse(model.isConsistent());
            assertFalse(model.isFixable());

            // remap a prefix and map the new URI to make it fixable
            HashMap<String, String> mappings = new HashMap<>();
            mappings.put("foo", "urn:foo2");
            mappings.put("bar2", "urn:bar2");
            assertFalse(registry.checkConsistency(root));
            model = model.setMappings(mappings);
            assertFalse(model.isConsistent());
            assertTrue(model.isFixable());

            // Apply the fixed model
            model = model.tryRegistryRepair();
            assertTrue(model.isConsistent());
            assertTrue(model.isFixable());
            assertFalse(registry.checkConsistency(root));
            model.apply(root);
            assertTrue(registry.checkConsistency(root));
            assertTrue(Objects.requireNonNull(NamespaceRegistryModel.create(root)).isConsistent());

            assertEquals(0, model.getDanglingPrefixes().size());
            assertEquals(0, model.getDanglingEncodedNamespaceUris().size());
            assertEquals(0, model.getRepairedMappings().size());

            // Check the extra mappings
            assertEquals("urn:foo2", registry.getURI("foo"));
            assertEquals("foo", registry.getPrefix("urn:foo2"));
            assertEquals("urn:bar2", registry.getURI("bar2"));
            assertEquals("bar2", registry.getPrefix("urn:bar2"));
        }
    }

    @Test
    public void testConsistencyCheckInvocationCount() throws Exception {
        Oak oak = new Oak()
                .with(new OpenSecurityProvider())
                .with(new InitialContent())
                .with(new NamespaceEditorProvider());
        try (ContentSession session = oak.createContentSession()) {
            Root root = session.getLatestRoot();
            ReadWriteNamespaceRegistry registry = new TestNamespaceRegistry(root);
            ReadWriteNamespaceRegistry spy = spy(registry);
            verify(spy, times(0)).checkConsistency(any(Root.class));
            new TestNamespaceRegistry(root);
            verify(spy, times(0)).checkConsistency(any(Root.class));
        }
    }

    static class TestNamespaceRegistry extends ReadWriteNamespaceRegistry {
        public TestNamespaceRegistry(Root root) {
            super(root);
        }

        @Override
        protected Root getWriteRoot() {
            return root;
        }
    }
}
