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
package org.apache.jackrabbit.oak.benchmark;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.util.Text;

/**
 * Concurrently reads random items from the deep tree where every 10th node is
 * access controlled.
 */
public class ConcurrentReadAccessControlledTreeTest extends ConcurrentReadDeepTreeTest {

    int counter = 0;

    public ConcurrentReadAccessControlledTreeTest(
            boolean runAsAdmin, int itemsToRead, boolean doReport) {
        super(runAsAdmin, itemsToRead, doReport);
    }

    @Override
    protected void visitingNode(Node node, int i) throws RepositoryException {
        super.visitingNode(node, i);
        String path = node.getPath();
        if (!path.contains("rep:policy")) {
            if (++counter == 10) {
                addPolicy(path, node);
                counter = 0;
            }
        }
    }

    private void addPolicy(String path, Node node) throws RepositoryException {
        AccessControlManager acMgr = node.getSession().getAccessControlManager();
        int level = 0;
        if (node.isNodeType(AccessControlConstants.NT_REP_POLICY)) {
            level = 1;
        } else if (node.isNodeType(AccessControlConstants.NT_REP_ACE)) {
            level = 2;
        } else if (node.isNodeType(AccessControlConstants.NT_REP_RESTRICTIONS)) {
            level = 3;
        }
        if (level > 0) {
            path = Text.getRelativeParent(path, level);
        }
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(node.getSession(), path);
        if (acl != null) {
            Privilege[] privileges = new Privilege[] {
                    acMgr.privilegeFromName(Privilege.JCR_READ),
                    acMgr.privilegeFromName(Privilege.JCR_READ_ACCESS_CONTROL)
            };
            if (acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privileges)) {
                acMgr.setPolicy(path, acl);
                node.getSession().save();
            }
        }
    }

}
