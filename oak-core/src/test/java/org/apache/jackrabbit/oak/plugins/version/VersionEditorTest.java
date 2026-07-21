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
package org.apache.jackrabbit.oak.plugins.version;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.JcrConstants.JCR_ISCHECKEDOUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class VersionEditorTest extends AbstractSecurityTest {

    private Tree versionable;
    private Tree child;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        Tree a = TreeUtil.addChild(root.getTree("/"), "a", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        Tree b = TreeUtil.addChild(a, "b", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        b.setProperty("prop", "value");
        // a non-versionable child that inherits the checked-in state from /a/b
        Tree c = TreeUtil.addChild(b, "c", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        c.setProperty("prop", "value");

        TreeUtil.addMixin(b, JcrConstants.MIX_VERSIONABLE,
                root.getTree(NodeTypeConstants.NODE_TYPES_PATH), null);
        root.commit();

        versionable = root.getTree("/a/b");
        child = root.getTree("/a/b/c");

        // check the versionable node in
        versionable.setProperty(JCR_ISCHECKEDOUT, Boolean.FALSE, Type.BOOLEAN);
        root.commit();
    }

    @Override
    @After
    public void after() throws Exception {
        try {
            root.refresh();
            Tree a = root.getTree("/a");
            if (a.exists()) {
                a.remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @Test
    public void changePropertyOnCheckedInNodeReportsPath() {
        versionable.setProperty("prop", "changed");
        assertCheckedInFailure("/a/b");
    }

    @Test
    public void addPropertyOnCheckedInNodeReportsPath() {
        versionable.setProperty("newProp", "value");
        assertCheckedInFailure("/a/b");
    }

    @Test
    public void deletePropertyOnCheckedInNodeReportsPath() {
        versionable.removeProperty("prop");
        assertCheckedInFailure("/a/b");
    }

    @Test
    public void changePropertyOnCheckedInChildReportsChildPath() {
        // the child inherits the read-only state; the reported path must be the
        // child's path, which exercises getPath() across multiple parent levels
        child.setProperty("prop", "changed");
        assertCheckedInFailure("/a/b/c");
    }

    @Test
    public void changeProtectedVersionPropertyReportsPath() {
        // changing a protected version property (other than jcr:isCheckedOut /
        // jcr:baseVersion) must fail; use the node's own uuid as the new
        // reference value so referential integrity is not violated first
        String uuid = versionable.getProperty(JcrConstants.JCR_UUID).getValue(Type.STRING);
        versionable.setProperty(JcrConstants.JCR_VERSIONHISTORY, uuid, Type.REFERENCE);
        try {
            root.commit();
            fail("Changing a protected version property must fail");
        } catch (CommitFailedException e) {
            assertEquals(CommitFailedException.CONSTRAINT, e.getType());
            assertEquals(100, e.getCode());
            assertTrue("Message should mention the protected property and the node path, but was: "
                            + e.getMessage(),
                    e.getMessage().endsWith("Property is protected: "
                            + JcrConstants.JCR_VERSIONHISTORY + " at /a/b"));
        }
    }

    private void assertCheckedInFailure(String expectedPath) {
        try {
            root.commit();
            fail("Modifying a checked-in node must fail");
        } catch (CommitFailedException e) {
            assertEquals(CommitFailedException.VERSION, e.getType());
            assertEquals(VersionExceptionCode.NODE_CHECKED_IN.ordinal(), e.getCode());
            assertTrue("Message should end with the node path, but was: " + e.getMessage(),
                    e.getMessage().endsWith(" at " + expectedPath));
        }
    }
}
