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

import java.util.UUID;

import javax.jcr.InvalidItemStateException;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;

import org.apache.jackrabbit.oak.benchmark.util.OakIndexUtils;

public class UUIDLookupTest extends AbstractTest {
    private static final int NODE_COUNT = 100;

    private int noOfIndex = Integer.getInteger("noOfIndex", 60);
    private boolean lookupByQuery = Boolean.getBoolean("lookupByQuery");

    private Session session;

    private Node root;

    protected Query createQuery(QueryManager manager, int i)
            throws RepositoryException {
        return manager.createQuery("SELECT * FROM [nt:base] WHERE [jcr:uuid] = " + i, Query.JCR_SQL2);
    }

    @Override
    public void beforeSuite() throws RepositoryException {
        session = getRepository().login(getCredentials());

        try {
            ensurePropertyIndexes();
        } catch (InvalidItemStateException e) {
            // some other oak instance probably created the same
            // index definition concurrently. refresh and try again
            // do not catch exception if it fails again.
            session.refresh(false);
            ensurePropertyIndexes();
        }

        root = session.getRootNode().addNode("testroot" + TEST_ID, "nt:unstructured");
        for (int i = 0; i < NODE_COUNT; i++) {
            Node node = root.addNode("node" + i, "nt:unstructured");
            node.setProperty("jcr:uuid", createUUID(i));
            session.save();
        }
        String lookupMode = lookupByQuery ? "query" : "Session#getNodeByIdentifier";
        System.out.printf("No of indexes (%s) %d, Lookup by (%s)[%s] %n",noOfIndex, noOfIndex, "lookupByQuery", lookupMode);
    }

    @Override
    public void runTest() throws Exception {
        if (lookupByQuery) {
            QueryManager manager = session.getWorkspace().getQueryManager();
            for (int i = 0; i < NODE_COUNT; i++) {
                Query query = createQuery(manager, i);
                NodeIterator iterator = query.execute().getNodes();
                while (iterator.hasNext()) {
                    Node node = iterator.nextNode();
                    if (node.getProperty("jcr:uuid").getLong() != i) {
                        throw new Exception("Invalid test result: " + node.getPath());
                    }
                }
            }
        } else {
            for (int i = 0; i < NODE_COUNT; i++) {
                session.getNodeByIdentifier(createUUID(i));
            }
        }
    }

    @Override
    public void afterSuite() throws RepositoryException {
        for (int i = 0; i < NODE_COUNT; i++) {
            root.getNode("node" + i).remove();
            session.save();
        }

        root.remove();
        session.save();
        session.logout();
    }

    private void ensurePropertyIndexes() throws RepositoryException {
        for (int i = 0; i < noOfIndex; i++) {
            new OakIndexUtils.PropertyIndex().
                    property("testcount"+i).
                    create(session);
        }
    }

    private static String createUUID(int i){
        return new UUID(0,i).toString();
    }
}
