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
package org.apache.jackrabbit.oak.plugins.nodetype.write;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.NamespaceRegistry;
import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.commons.cnd.CompactNodeTypeDefReader;
import org.apache.jackrabbit.commons.cnd.DefinitionBuilderFactory;
import org.apache.jackrabbit.commons.cnd.TemplateBuilderFactory;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.GlobalNameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.name.ReadWriteNamespaceRegistry;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.JcrConstants.NT_RESOURCE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.NODE_TYPES_PATH;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertThat;

public class NodeTypeRegistryTest {
    private ContentRepository repository = null;
    private Root root;
    private ContentSession session = null;
    
    @Before
    public void setUp() throws LoginException, NoSuchWorkspaceException {
        repository = new Oak().with(new InitialContent()).with(new OpenSecurityProvider())
            .with(new TypeEditorProvider()).createContentRepository();
        session = repository.login(null, null);
        root = session.getLatestRoot();
    }
    
    @After
    public void tearDown() throws IOException {
        if (session != null) {
            session.close();
        }
        if (repository instanceof Closeable) {
            ((Closeable) repository).close();
        }
        repository = null;
    }
    
    // OAK-7886
    @Ignore("OAK-7886")
    @Test
    public void reRegisterNtResource() throws Exception {
        NodeTypeManager ntMgr = new ReadWriteNodeTypeManager() {
            @Override
            protected Tree getTypes() {
                return root.getTree(NODE_TYPES_PATH);
            }

            @Nonnull
            @Override
            protected Root getWriteRoot() {
                return root;
            }
        };
        ValueFactory vf = new ValueFactoryImpl(
                root, new NamePathMapperImpl(new GlobalNameMapper(root)));
        NamespaceRegistry nsReg = new ReadWriteNamespaceRegistry(root) {
            @Override
            protected Root getWriteRoot() {
                return root;
            }
        };
        DefinitionBuilderFactory<NodeTypeTemplate, NamespaceRegistry> factory
                = new TemplateBuilderFactory(ntMgr, vf, nsReg);

        NodeType ntResource = ntMgr.getNodeType(NT_RESOURCE);
        List<String> supertypeNames = Arrays.asList(ntResource.getDeclaredSupertypeNames());
        assertThat(supertypeNames, hasItem(NT_BASE));

        List<NodeTypeTemplate> templates;
        InputStream in = NodeTypeRegistryTest.class.getResourceAsStream("ntResource.cnd");
        try {
            CompactNodeTypeDefReader<NodeTypeTemplate, NamespaceRegistry> reader
                    = new CompactNodeTypeDefReader<NodeTypeTemplate, NamespaceRegistry>(
                    new InputStreamReader(in, UTF_8), "ntResource.cnd", factory);
            templates = reader.getNodeTypeDefinitions();
        } finally {
            in.close();
        }
        for (NodeTypeTemplate t : templates) {
            ntMgr.registerNodeType(t, true);
        }

        ntResource = ntMgr.getNodeType(NT_RESOURCE);
        supertypeNames = Arrays.asList(ntResource.getDeclaredSupertypeNames());
        assertThat(supertypeNames, hasItem(NT_BASE));
    }
}
