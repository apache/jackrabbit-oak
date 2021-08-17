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
package org.apache.jackrabbit.oak.security.authorization.restriction;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.jetbrains.annotations.NotNull;

import javax.jcr.RepositoryException;
import javax.jcr.ValueFactory;
import javax.jcr.security.AccessControlManager;
import java.security.Principal;

public abstract class AbstractRestrictionTest extends AbstractSecurityTest {

    ValueFactory vf;
    ContentSession testSession;
    Principal testPrincipal;
    
    @Override
    public void before() throws Exception {
        super.before();

        Tree rootTree = root.getTree("/");

        // "/a/d/b/e/c/f"
        Tree a = TreeUtil.addChild(rootTree, "a", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        a.setProperty("propA", "value");
        Tree d = TreeUtil.addChild(a, "d", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        Tree b = TreeUtil.addChild(d, "b", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        Tree e = TreeUtil.addChild(b, "e", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        Tree c = TreeUtil.addChild(e, "c", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        Tree f = TreeUtil.addChild(c, "f", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        c.setProperty("prop", "value");
        c.setProperty("a", "value");

        testPrincipal = getTestUser().getPrincipal();

        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/a");

        vf = getValueFactory(root);
        if (addEntry(acl)) {
            acMgr.setPolicy(acl.getPath(), acl);
        }
        root.commit();
        testSession = createTestSession();
    }
    
    abstract boolean addEntry(@NotNull JackrabbitAccessControlList acl) throws RepositoryException;

    @Override
    public void after() throws Exception {
        try {
            testSession.close();
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
}