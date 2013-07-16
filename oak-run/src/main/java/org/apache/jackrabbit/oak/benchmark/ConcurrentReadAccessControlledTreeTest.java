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

import javax.jcr.ItemVisitor;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;
import javax.jcr.util.TraversingItemVisitor;

import org.apache.jackrabbit.core.security.principal.EveryonePrincipal;

/**
 * Concurrently reads random items from the deep tree where every 10th node is
 * access controlled.
 */
public class ConcurrentReadAccessControlledTreeTest extends AbstractDeepTreeTest {

    private int bgReaders = 20;
    private int cnt = 10000;

    @Override
    protected void beforeSuite() throws Exception {
        super.beforeSuite();

        ItemVisitor visitor = new TraversingItemVisitor.Default() {
            int counter = 0;
            @Override
            protected void entering(Node node, int level) throws RepositoryException {
                if (++counter == 10) {
                    addPolicy(node);
                    counter = 0;
                }
                super.entering(node, level);
            }

            private void addPolicy(Node node) throws RepositoryException {
                AccessControlManager acMgr = node.getSession().getAccessControlManager();
                String path = node.getPath();
                AccessControlPolicyIterator acIterator = acMgr.getApplicablePolicies(path);
                if (acIterator.hasNext()) {
                    AccessControlPolicy policy = acIterator.nextAccessControlPolicy();
                    if (policy instanceof AccessControlList) {
                        AccessControlList acl = (AccessControlList) policy;
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
        };

        visitor.visit(testRoot);

        for (int i = 0; i < bgReaders; i++) {
            addBackgroundJob(new RandomRead(loginAnonymous(), false));
        }
    }

    @Override
    protected void runTest() throws Exception {
        Session testSession = loginAnonymous();
        RandomRead randomRead = new RandomRead(testSession, true);
        randomRead.run();
        testSession.logout();
    }

    private class RandomRead implements Runnable {

        private final Session testSession;
        private final boolean doReport;

        private RandomRead(Session testSession, boolean doReport) {
            this.testSession = testSession;
            this.doReport = doReport;
        }
        public void run() {
            try {
                randomRead(testSession, allPaths, cnt, doReport);
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        }
    }
}