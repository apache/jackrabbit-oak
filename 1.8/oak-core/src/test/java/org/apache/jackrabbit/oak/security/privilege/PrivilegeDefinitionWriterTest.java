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

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.ImmutablePrivilegeDefinition;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.junit.After;
import org.junit.Test;

import com.google.common.collect.Iterables;
import org.mockito.Mockito;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;

public class PrivilegeDefinitionWriterTest extends AbstractSecurityTest implements PrivilegeConstants {

    @After
    @Override
    public void after() throws Exception {
        try {
            root.refresh();
        } finally {
            super.after();
        }
    }

    @Test
    public void testNameCollision() {
        try {
            PrivilegeDefinitionWriter writer = new PrivilegeDefinitionWriter(root);
            writer.writeDefinition(new ImmutablePrivilegeDefinition(JCR_READ, true, null));
            fail("name collision");
        } catch (RepositoryException e) {
            // success
        }
    }

    @Test
    public void testMissingPrivilegeRoot() throws Exception {
        ContentRepository repo = new Oak().with(new OpenSecurityProvider()).createContentRepository();
        Root tmpRoot = repo.login(null, null).getLatestRoot();
        try {
            PrivilegeDefinitionWriter writer = new PrivilegeDefinitionWriter(tmpRoot);
            writer.writeDefinition(new ImmutablePrivilegeDefinition("newName", true, null));
            fail("missing privilege root");
        } catch (RepositoryException e) {
            // success
        } finally {
            tmpRoot.getContentSession().close();
        }
    }

    @Test
    public void testWriteDefinition() throws Exception {
        PrivilegeDefinitionWriter writer = new PrivilegeDefinitionWriter(root);
        writer.writeDefinition(new ImmutablePrivilegeDefinition(
                "tmp", true, asList(JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL)));

        Tree privRoot = root.getTree(PRIVILEGES_PATH);
        assertTrue(privRoot.hasChild("tmp"));

        Tree tmpTree = privRoot.getChild("tmp");
        assertTrue(TreeUtil.getBoolean(tmpTree, REP_IS_ABSTRACT));
        assertArrayEquals(
                new String[] {JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL},
                Iterables.toArray(TreeUtil.getStrings(tmpTree, REP_AGGREGATES), String.class));
    }

    @Test(expected = RepositoryException.class)
    public void testCommitFails() throws Exception {
        Root r = Mockito.spy(root);
        doThrow(new CommitFailedException(CommitFailedException.OAK, 1, "msg")).when(r).commit();


        PrivilegeDefinitionWriter writer = new PrivilegeDefinitionWriter(r);
        writer.writeDefinition(new ImmutablePrivilegeDefinition(
                "tmp", true, asList(JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL)));
    }
}