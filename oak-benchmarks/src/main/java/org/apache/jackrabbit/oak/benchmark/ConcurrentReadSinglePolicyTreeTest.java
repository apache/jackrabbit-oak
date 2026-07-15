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
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;

/**
 * Concurrently reads random items from the deep tree where only the root node is
 * access controlled.
 */
public class ConcurrentReadSinglePolicyTreeTest extends ConcurrentReadDeepTreeTest {

    public ConcurrentReadSinglePolicyTreeTest(
            boolean runAsAdmin, int itemsToRead, boolean doReport) {
        super(runAsAdmin, itemsToRead, doReport);
    }

    @Override
    protected void visitingNode(Node node, int i) throws RepositoryException {
        super.visitingNode(node, i);
        String path = node.getPath();
        AccessControlManager acMgr = node.getSession().getAccessControlManager();
        if (testRoot.getPath().equals(path)) {
            JackrabbitAccessControlList policy = AccessControlUtils.getAccessControlList(acMgr, path);
            if (policy != null) {
                policy.addEntry(EveryonePrincipal.getInstance(), AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_READ), true);
            }
            acMgr.setPolicy(path, policy);
        } else if (!path.contains("rep:policy")) {
            for (AccessControlPolicy policy : acMgr.getPolicies(path)) {
                if (policy instanceof JackrabbitAccessControlList) {
                    acMgr.removePolicy(path, policy);
                }
            }
        }
        node.getSession().save();
    }
}
