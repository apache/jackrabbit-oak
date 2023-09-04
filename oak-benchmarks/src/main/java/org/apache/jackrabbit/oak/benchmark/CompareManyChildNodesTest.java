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

import org.apache.jackrabbit.oak.fixture.OakFixture;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.json.JsopDiff;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import static org.apache.commons.lang3.reflect.FieldUtils.readField;
import static org.apache.commons.lang3.reflect.FieldUtils.writeField;
import static org.apache.commons.lang3.reflect.MethodUtils.invokeMethod;

/**
 * Test for measuring the performance of comparing node with many children and
 * each child having multiple properties.
 */
public class CompareManyChildNodesTest extends AbstractTest {
    
    private static final String ROOT_NODE_NAME = "compare" + TEST_ID;

    private static final int CHILD_COUNT = 10_000;

    private Session session;

    private DocumentNodeStore store;
    private DocumentNodeState before;
    private DocumentNodeState after;

    @Override
    public void beforeSuite() throws Exception {
        session = getRepository().login(getCredentials());
        final Node node = session.getRootNode().addNode(ROOT_NODE_NAME, "nt:unstructured");
        for (int i = 0; i < CHILD_COUNT; i++) {
            node.addNode("node" + i, "nt:unstructured");
            if (i % 1000 == 0) {
                session.save();
            }
        }

        session.save();
        store = getDocumentStore();
        writeField(store, "disableJournalDiff", true, true);
        before = store.getRoot();

        for (int i = 0; i < CHILD_COUNT; i++) {
            node.addNode("one_more" + i, "nt:unstructured");
            Node n = node.getNode("node" + i);
            for (int j = 0; j < 10; j++) {
                n.setProperty("property_"+j, "property_"+j);
            }
        }

        session.save();
        after = store.getRoot();
        invalidateCache();
    }

    private void invalidateCache() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        if (store != null) {
            invokeMethod(store.getDiffCache(), true, "invalidateAll");
        }
    }

    private DocumentNodeStore getDocumentStore() throws IllegalAccessException {
        final RepositoryFixture fixture = getCurrentFixture();
        if (fixture instanceof OakRepositoryFixture) {
            OakRepositoryFixture oakRepositoryFixture = (OakRepositoryFixture) fixture;
            OakFixture oakFixture = oakRepositoryFixture.getOakFixture();
            if (oakFixture instanceof OakFixture.MongoFixture) {
                OakFixture.MongoFixture mongoFixture = (OakFixture.MongoFixture) oakFixture;
                return ((List<DocumentNodeStore>) readField(mongoFixture, "nodeStores", true)).get(0);
            }
        }
        throw new IllegalArgumentException("Fixture " + fixture + " not supported for this benchmark. " +
                "Only Mongo Fixture is supported.");
    }


    @Override
    public void beforeTest() throws Exception {
        invalidateCache();
    }

    @Override
    public void runTest() throws Exception {
        after.compareAgainstBaseState(before, new JsopDiff());
    }

    @Override
    public void afterSuite() throws RepositoryException {
        session.getRootNode().getNode(ROOT_NODE_NAME).remove();
        session.save();
        session.logout();
    }
}
