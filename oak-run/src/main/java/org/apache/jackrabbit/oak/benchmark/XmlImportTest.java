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

import java.io.InputStream;
import javax.jcr.ImportUUIDBehavior;
import javax.jcr.Node;
import javax.jcr.Session;

public class XmlImportTest extends AbstractTest {

    private Session adminSession;
    private Node testRoot;
    private int counter;

    @Override
    protected void runTest() throws Exception {
        InputStream in = getClass().getClassLoader().getResourceAsStream("deepTree.xml");
        adminSession.importXML(testRoot.getPath(), in, ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW);
        adminSession.save();
    }

    @Override
    protected void beforeTest() throws Exception {
        adminSession = loginWriter();
        String name = getClass().getSimpleName() + TEST_ID + counter++;
        testRoot = adminSession.getRootNode().addNode(name, "nt:unstructured");
        adminSession.save();
    }

    @Override
    protected void afterTest() throws Exception {
        adminSession.logout();
    }
}