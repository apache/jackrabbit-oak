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

import java.util.Random;
import java.util.UUID;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;

/**
 * This benchmark measures the read performance of child nodes using
 * an ORDER BY query.
 * <p>
 * This is related to OAK-1263.
 * 
 */
public class OrderByQueryTest extends AbstractTest {

	private static final String NT = "oak:unstructured";

	private static final String ROOT_NODE_NAME = "test" + TEST_ID;
	private static final int NUM_NODES = 10000;
	private static final String PROPERTY_NAME = "testProperty";
	private static final Random random = new Random(); // doesn't have to be very secure, just some randomness

    @Override
    protected void beforeSuite() throws Exception {
        Session session = loginWriter();
        Node rootNode = session.getRootNode();
        if (rootNode.hasNode(ROOT_NODE_NAME)) {
            Node root = rootNode.getNode(ROOT_NODE_NAME);
            root.remove();
        }
        rootNode = session.getRootNode().addNode(ROOT_NODE_NAME, NT);
        
        for (int i = 0; i < NUM_NODES; i++) {
        	if (i%1000==0) {
        		session.save();
        	}
            Node newNode = rootNode.addNode(UUID.randomUUID().toString(), NT);
            newNode.setProperty(PROPERTY_NAME, random.nextLong());
        }
        session.save();
    }

    @Override
    public void runTest() throws Exception {
        final Session session = loginWriter();
        try {
            // run the query
            final QueryManager qm = session.getWorkspace().getQueryManager();

            final Query q =
                    qm.createQuery("SELECT * FROM [oak:unstructured] AS s WHERE "
                    		+ "ISDESCENDANTNODE(s, [/"+ROOT_NODE_NAME+"/]) ORDER BY s."+PROPERTY_NAME+"]",
                    		Query.JCR_SQL2);
            final QueryResult res = q.execute();

            final NodeIterator nit = res.getNodes();
//            while(nit.hasNext()) {
//            	Node node = nit.nextNode();
////            	System.out.println("node: "+node.getPath()+", prop="+node.getProperty(PROPERTY_NAME).getLong());
//            }
        } catch (RepositoryException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
