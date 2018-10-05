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
package org.apache.jackrabbit.oak.jcr.security.authorization;

import javax.jcr.Node;
import javax.jcr.security.AccessControlException;

public class AccessControlWorkspaceImporterTest extends AccessControlImporterTest {

    protected boolean isSessionImport() {
        return false;
    }

    /**
     * Make sure repo-level acl is not imported below any other node than the
     * root node.
     *
     * @throws Exception
     */
    public void testImportRepoACLAtTestNode() throws Exception {
        try {
            Node target = testRootNode.addNode("test");
            target.addMixin("rep:RepoAccessControllable");
            superuser.save();

            doImport(target.getPath(), XML_REPO_POLICY_TREE);
            fail("Importing repo policy to non-root node must fail");
        } catch (AccessControlException e) {
            // success
        } finally {
            superuser.refresh(false);
        }
    }
}
