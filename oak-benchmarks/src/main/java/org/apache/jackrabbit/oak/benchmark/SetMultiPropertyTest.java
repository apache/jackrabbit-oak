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
import javax.jcr.RepositoryException;
import javax.jcr.Session;

/**
 * Test for measuring the performance of setting a single multi valued property and
 * saving the change.
 */
public class SetMultiPropertyTest extends AbstractTest {
    private static final String[] VALUES = createValues(100);

    private static String[] createValues(int count) {
        String[] values = new String[count];
        for (int k = 0; k < values.length; k++) {
            values[k] = "value" + k;
        }
        return values;
    }

    private Session session;

    private Node node;
    
    String testNodeName = "test" + TEST_ID;

    @Override
    public void beforeSuite() throws RepositoryException {
        session = getRepository().login(getCredentials());
        node = session.getRootNode().addNode(testNodeName, "nt:unstructured");
        session.save();
    }

    @Override
    public void beforeTest() throws RepositoryException {
        node.setProperty("count", new String[0]);
        session.save();
    }

    @Override
    public void runTest() throws Exception {
        for (int i = 0; i < 1000; i++) {
            node.setProperty("count", VALUES);
            session.save();
        }
    }

    @Override
    public void afterTest() throws RepositoryException {
    }

    @Override
    public void afterSuite() throws RepositoryException {
        session.getRootNode().getNode(testNodeName).remove();
        session.save();
        session.logout();
    }

}
