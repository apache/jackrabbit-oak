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
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.junit.Test;

public class MiscTest extends AbstractRepositoryTest {

    public MiscTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2024">OAK-2024</a>
     */
    @Test
    public void testTraverseIndex() throws Exception {
        Session session = getAdminSession();
        AccessControlUtils.denyAllToEveryone(session, "/oak:index");
        session.save();
        Node index = session.getNode("/oak:index");
        traverse(index);
    }

    private void traverse(Node node) throws RepositoryException {
        PropertyIterator iter = node.getProperties();
        while (iter.hasNext()) {
            Property p = iter.nextProperty();
            p.getDefinition();
        }
        NodeIterator niter = node.getNodes();
        while (niter.hasNext()) {
            traverse(niter.nextNode());
        }
    }
}