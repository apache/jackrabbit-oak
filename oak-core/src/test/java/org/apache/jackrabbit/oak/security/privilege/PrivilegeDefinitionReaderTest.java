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
package org.apache.jackrabbit.oak.security.privilege;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NT_OAK_UNSTRUCTURED;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class PrivilegeDefinitionReaderTest extends AbstractSecurityTest implements PrivilegeConstants {

    private PrivilegeDefinitionReader reader;

    @Before
    public void before() throws Exception {
        super.before();

        Tree privilegeDefs = root.getTree(PRIVILEGES_PATH);
        TreeUtil.addChild(privilegeDefs, "invalid", NT_OAK_UNSTRUCTURED);

        reader = new PrivilegeDefinitionReader(root);
    }

    @Test
    public void testReadInvalidDefinition() throws Exception {
        assertNull(reader.readDefinition("invalid"));
    }

    @Test
    public void testReadNonExisting() {
        assertNull(reader.readDefinition("nonexisting"));
    }

    @Test
    public void testReadDefinition() {
        assertNotNull(reader.readDefinition(JCR_READ));
    }

    @Test
    public void testReadDefinitionsIgnoresInvalid() {
        assertFalse(reader.readDefinitions().containsKey("invalid"));
    }

    @Test
    public void testMissingPermissionRoot() throws Exception {
        ContentRepository repo = new Oak().with(new OpenSecurityProvider()).createContentRepository();
        Root tmpRoot = repo.login(null, null).getLatestRoot();
        try {
            PrivilegeDefinitionReader reader = new PrivilegeDefinitionReader(tmpRoot);
            assertNull(reader.readDefinition(JCR_READ));
        } finally {
            tmpRoot.getContentSession().close();
        }
    }
}
