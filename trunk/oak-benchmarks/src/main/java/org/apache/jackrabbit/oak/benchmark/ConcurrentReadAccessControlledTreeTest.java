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

import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;

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
            AccessControlManager acMgr = node.getSession().getAccessControlManager();
            Privilege[] privileges = new Privilege[] {
                    acMgr.privilegeFromName(Privilege.JCR_READ),
                    acMgr.privilegeFromName(Privilege.JCR_READ_ACCESS_CONTROL)
            };
            if (++counter == 10) {
                addPolicy(acMgr, node, privileges, EveryonePrincipal.getInstance());
                counter = 0;
            }
        }
    }
}
