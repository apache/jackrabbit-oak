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

import java.util.UUID;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

public class StringWriteTest extends AbstractTest<Object> {

    private static final int PROPERTY_COUNT = Integer.getInteger("StringWriteTest.propertyCount", 100);

    private static String randomString() {
        return UUID.randomUUID().toString();
    }

    private Session session;

    private Node root;

    @Override
    public void beforeSuite() throws RepositoryException {
        session = loginWriter();
    }

    @Override
    protected void beforeTest() throws Exception {
        root = session.getRootNode().addNode("StringWrite" + TEST_ID, "nt:unstructured");
        session.save();
    }

    @Override
    protected void runTest() throws Exception {
        for (int i = 0; i < PROPERTY_COUNT; i++) {
            root.setProperty(randomString(), randomString());
        }
        session.save();
    }

    @Override
    protected void afterTest() throws Exception {
        root.remove();
        session.save();
    }
}

