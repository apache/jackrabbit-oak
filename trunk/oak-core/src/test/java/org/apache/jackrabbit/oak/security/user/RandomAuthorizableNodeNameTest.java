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
package org.apache.jackrabbit.oak.security.user;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableNodeName;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RandomAuthorizableNodeNameTest extends AbstractSecurityTest {

    private final String id = "id";

    private AuthorizableNodeName nameGenerator = new RandomAuthorizableNodeName();

    @Override
    public void after() throws Exception {
        try {
            Authorizable a = getUserManager(root).getAuthorizable(id);
            if (a != null) {
                a.remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        ConfigurationParameters userConfig = ConfigurationParameters.of(UserConstants.PARAM_AUTHORIZABLE_NODE_NAME, nameGenerator);
        return ConfigurationParameters.of(UserConfiguration.NAME, userConfig);
    }

    private void assertNodeName(Authorizable authorizable) throws RepositoryException {
        assertEquals(id, authorizable.getID());

        String path = authorizable.getPath();
        Tree tree = root.getTree(path);

        assertFalse(id.equals(tree.getName()));
        assertEquals(RandomAuthorizableNodeName.DEFAULT_LENGTH, tree.getName().length());
    }

    private void assertNodeName(Authorizable authorizable, String relPath) throws RepositoryException {
        assertEquals(id, authorizable.getID());

        String path = authorizable.getPath();
        Tree tree = root.getTree(path);

        assertFalse(id.equals(tree.getName()));
        assertEquals(RandomAuthorizableNodeName.DEFAULT_LENGTH, tree.getName().length());

        String end = '/' + relPath + '/' + tree.getName();
        assertTrue(path.endsWith(end));
    }

    @Test
    public void testGenerateNodeName() {
        String nodeName = nameGenerator.generateNodeName(id);

        assertFalse("id".equals(nodeName));
        assertEquals(RandomAuthorizableNodeName.DEFAULT_LENGTH, nodeName.length());
        assertFalse(nodeName.equals(nameGenerator.generateNodeName(id)));
    }

    @Test
    public void testCreateUser() throws Exception {
        User user = getUserManager(root).createUser(id, "pw");
        root.commit();

        assertNodeName(user);

        Authorizable authorizable = getUserManager(root).getAuthorizable(id);
        assertNodeName(authorizable);
    }

    @Test
    public void testCreateUserWithPath() throws Exception {
        User user = getUserManager(root).createUser(id, "pw", new PrincipalImpl(id), "a/b");
        root.commit();

        assertNodeName(user, "a/b");
    }

    @Test
    public void testCreateGroup() throws Exception {
        Group group = getUserManager(root).createGroup(id);
        root.commit();

        assertNodeName(group);

        Authorizable authorizable = getUserManager(root).getAuthorizable(id);
        assertNodeName(authorizable);
    }

    @Test
    public void testCreateGroupWithPath() throws Exception {
        Group group = getUserManager(root).createGroup(id, new PrincipalImpl(id), "a/b");
        root.commit();

        assertNodeName(group, "a/b");
    }
}