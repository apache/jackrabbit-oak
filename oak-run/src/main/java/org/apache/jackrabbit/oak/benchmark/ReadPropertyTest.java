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
import javax.jcr.Session;

/**
 * {@code ReadPropertyTest} implements a performance test, which reads
 * three properties: one with a jcr prefix, one with the empty prefix and a
 * third one, which does not exist.
 */
public class ReadPropertyTest extends AbstractTest {

    private Session session;

    private Node root;

    @Override
    protected void beforeSuite() throws Exception {
        session = getRepository().login(getCredentials());
        root = session.getRootNode().addNode(
                getClass().getSimpleName() + TEST_ID, "nt:unstructured");
        root.setProperty("property", "value");
        session.save();
    }

    @Override
    protected void runTest() throws Exception {
        for (int i = 0; i < 10000; i++) {
            root.getProperty("jcr:primaryType");
            root.getProperty("property");
            root.hasProperty("does-not-exist");
        }
    }

    @Override
    protected void afterSuite() throws Exception {
        root.remove();
        session.save();
        session.logout();
    }
}
