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

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import javax.jcr.ItemVisitor;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;
import javax.jcr.util.TraversingItemVisitor;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.util.Text;

/**
 * Concurrently reads random items from the deep tree where every 100th node
 * is access controlled and each policy node contains 100 ACEs for different
 * principals.
 */
public class ConcurrentReadAccessControlledTreeTest2 extends ConcurrentReadDeepTreeTest {

    List<Principal> principals = new ArrayList();

    public ConcurrentReadAccessControlledTreeTest2(
            boolean runAsAdmin, int itemsToRead, int bgReaders, boolean doReport) {
        super(runAsAdmin, itemsToRead, bgReaders, doReport);
    }

    @Override
    protected void beforeSuite() throws Exception {
        super.beforeSuite();

        UserManager uMgr = ((JackrabbitSession) adminSession).getUserManager();
        for (int i = 0; i < 100; i++) {
            Authorizable a = uMgr.getAuthorizable("group" + i);
            if (a == null) {
                a = uMgr.createGroup("group" + i);
                principals.add(a.getPrincipal());
            }
        }

        ItemVisitor visitor = new TraversingItemVisitor.Default() {
            int counter = 0;
            @Override
            protected void entering(Node node, int level) throws RepositoryException {
                if (++counter == 100) {
                    addPolicy(node);
                    counter = 0;
                }
                super.entering(node, level);
            }

            private void addPolicy(Node node) throws RepositoryException {
                AccessControlManager acMgr = node.getSession().getAccessControlManager();
                String path = node.getPath();
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
                AccessControlPolicyIterator acIterator = acMgr.getApplicablePolicies(path);
                if (acIterator.hasNext()) {
                    AccessControlPolicy policy = acIterator.nextAccessControlPolicy();
                    if (policy instanceof AccessControlList) {
                        AccessControlList acl = (AccessControlList) policy;
                        Privilege[] privileges = new Privilege[] {
                                acMgr.privilegeFromName(Privilege.JCR_READ),
                                acMgr.privilegeFromName(Privilege.JCR_READ_ACCESS_CONTROL)
                        };
                        for (Principal principal : principals) {
                            acl.addAccessControlEntry(principal, privileges);
                        }
                        acMgr.setPolicy(path, acl);
                        adminSession.save();
                    }
                }
            }
        };

        visitor.visit(testRoot);
    }

}
