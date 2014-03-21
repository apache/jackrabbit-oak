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
package org.apache.jackrabbit.oak.plugins.document;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.junit.After;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicDocumentStoreTest extends AbstractDocumentStoreTest {

    private static final Logger LOG = LoggerFactory.getLogger(BasicDocumentStoreTest.class);

    private Set<String> removeMe = new HashSet<String>();

    public BasicDocumentStoreTest(DocumentStore ds) {
        super(ds);
    }

    @After
    public void cleanUp() {
        for (String id : removeMe) {
            try {
                super.ds.remove(Collection.NODES, id);
            } catch (Exception ex) {
                // best effort
            }
        }
    }

    @Test
    public void testAddAndRemove() {
        String id = this.getClass().getName() + "-foobar";

        // remove if present
        NodeDocument nd = super.ds.find(Collection.NODES, id);
        if (nd != null) {
            super.ds.remove(Collection.NODES, id);
        }

        // add
        UpdateOp up = new UpdateOp(id, true);
        up.set("_id", id);
        assertTrue(super.ds.create(Collection.NODES, Collections.singletonList(up)));
        removeMe.add(id);
    }

    @Test
    public void testMaxId() {
        // TODO see OAK-1589
        Assume.assumeTrue(!(super.ds instanceof MongoDocumentStore));
        int min = 0;
        int max = 32768;
        int test = 0;

        while (max - min >= 2) {
            test = (max + min) / 2;
            String id = generateId(test);
            UpdateOp up = new UpdateOp(id, true);
            up.set("_id", id);
            boolean success = super.ds.create(Collection.NODES, Collections.singletonList(up));
            if (success) {
                // check that we really can read it
                NodeDocument findme = super.ds.find(Collection.NODES, id, 0);
                assertNotNull("failed to retrieve previously stored document", findme);
                super.ds.remove(Collection.NODES, id);
                min = test;
            } else {
                max = test;
            }
        }

        LOG.info("max id for " + super.ds.getClass() + " was " + test);
    }

    private static String generateId(int length) {
        StringBuffer buf = new StringBuffer();
        while (length-- > 0) {
            buf.append('0' + (length % 10));
        }
        return buf.toString();
    }
}
