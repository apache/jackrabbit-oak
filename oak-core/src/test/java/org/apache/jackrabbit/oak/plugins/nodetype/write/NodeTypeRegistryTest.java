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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.of;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_FOLDER;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.MIX_INDEXABLE;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.Nonnull;
import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

public class NodeTypeRegistryTest {
    private ContentRepository repository = null;
    private Root root;
    private ContentSession session = null;
    
    static void registerNodeType(@Nonnull Root root, @Nonnull String resourceName) throws IOException {
        checkArgument(!Strings.isNullOrEmpty(resourceName));
        checkNotNull(root);

        InputStream stream = null;

        try {
            stream = NodeTypeRegistryTest.class.getResourceAsStream(resourceName);
            NodeTypeRegistry.register(root, stream, NodeTypeRegistryTest.class.getName());            
        } finally {
            if (stream != null) {
                stream.close();
            }
            
        }
    }
    
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
    
    @Test(expected = CommitFailedException.class)
    public void oakIndexableFailing() throws IOException, CommitFailedException {
        registerNodeType(root, "oak3725-1.cnd");
        
        Tree test = root.getTree("/").addChild("test");
        test.setProperty(JCR_PRIMARYTYPE, NT_FOLDER, NAME);            
        test.addChild("oak:index").setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, NAME);
        root.commit();
    }
    
    @Test
    public void oakIndexableSuccessful() throws IOException, CommitFailedException {
        registerNodeType(root, "oak3725-2.cnd");
        
        Tree test = root.getTree("/").addChild("test");
        test.setProperty(JCR_PRIMARYTYPE, NT_FOLDER, NAME);
        test.setProperty(JCR_MIXINTYPES, of(MIX_INDEXABLE), Type.NAMES);
        test.addChild("oak:index").setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, NAME);
        root.commit();
    }
}
