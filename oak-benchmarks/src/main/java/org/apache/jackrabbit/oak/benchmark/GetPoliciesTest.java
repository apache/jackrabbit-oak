/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.benchmark;

import javax.jcr.Node;
import javax.jcr.Session;
import javax.jcr.security.AccessControlManager;

import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;

import static javax.jcr.security.Privilege.JCR_READ;
import static org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils.addAccessControlEntry;

/**
 * {@code GetPoliciesTest} implements a performance test, which get policies from the
 * repository.
 */
public class GetPoliciesTest extends AbstractTest {

    private Session session;
    private Node testRoot;
    private String path;
    private AccessControlManager acm;

    @Override
    protected void beforeSuite() throws Exception {
        session = loginWriter();
        testRoot = session.getRootNode().addNode(
                getClass().getSimpleName() + TEST_ID, "nt:unstructured");

        Node n = testRoot.addNode("node1");
        path = n.getPath();
        addAccessControlEntry(session, n.getPath(),
                EveryonePrincipal.getInstance(), new String[] { JCR_READ },
                true);
        
        session.save();
        
        testRoot = loginWriter().getNode(testRoot.getPath());
        acm = testRoot.getSession().getAccessControlManager();
    }

    @Override
    protected void runTest() throws Exception {
        for (int i = 0; i < 10000; i++) {
            acm.getPolicies(path);
        }

    }

    @Override
    protected void afterSuite() throws Exception {
        testRoot.remove();
        session.save();
        session.logout();
    }
}
