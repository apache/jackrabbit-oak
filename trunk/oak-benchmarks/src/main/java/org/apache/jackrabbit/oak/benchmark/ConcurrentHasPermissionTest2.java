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
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;

/**
 * Concurrently calls Session#hasPermission on the deep tree where every 100th node
 * is access controlled and each policy node contains 100 ACEs for different
 * principals. The hasPermission methods is calles as follows:
 *
 * - the path argument a random path out of the deep tree
 * - the actions are randomly selected from the combinations listed in {@link #ACTIONS}
 */
public class ConcurrentHasPermissionTest2 extends ConcurrentHasPermissionTest {

    int counter = 0;
    final List<Principal> principals = new ArrayList<Principal>();

    public ConcurrentHasPermissionTest2(boolean runAsAdmin, int itemsToRead, boolean doReport) {
        super(runAsAdmin, itemsToRead, doReport);
    }

    @Override
    protected void createDeepTree() throws Exception {
        UserManager uMgr = ((JackrabbitSession) adminSession).getUserManager();
        for (int i = 0; i < 100; i++) {
            Authorizable a = uMgr.getAuthorizable("group" + i);
            if (a == null) {
                a = uMgr.createGroup("group" + i);
                principals.add(a.getPrincipal());
            }
        }
        super.createDeepTree();
    }

    @Override
    protected void visitingNode(Node node, int i) throws RepositoryException {
        super.visitingNode(node, i);

        AccessControlManager acMgr = node.getSession().getAccessControlManager();
        Privilege[] privileges = new Privilege[] {
                acMgr.privilegeFromName(Privilege.JCR_READ),
                acMgr.privilegeFromName(Privilege.JCR_READ_ACCESS_CONTROL)
        };
        if (!node.getPath().contains("rep:policy")) {
            if (++counter == 100) {
                addPolicy(acMgr, node, privileges, principals);
                counter = 0;
            }
        }
    }
}
