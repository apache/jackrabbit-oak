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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import com.mongodb.BasicDBObject;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.MongoUtils.isAvailable;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

public class MongoStatusTest {

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    private MongoStatus status;

    @BeforeClass
    public static void mongoAvailable() {
        assumeTrue(isAvailable());
    }

    @Before
    public void createStatus() {
        MongoConnection c = connectionFactory.getConnection();
        status = new MongoStatus(c.getDB());
    }

    @Test
    public void testDetails() {
        String details = status.getServerDetails();
        assertNotNull(details);
        assertFalse(details.isEmpty());
        assertTrue(details.startsWith("{"));
        assertTrue(details.endsWith("}"));
        assertTrue(details.contains("host="));
    }

    @Test
    public void testReadConcern() {
        BasicDBObject mockServerStatus = new BasicDBObject();
        BasicDBObject storageEngine = new BasicDBObject();
        status.setServerStatus(mockServerStatus);

        assertFalse(status.isMajorityReadConcernSupported());

        mockServerStatus.put("storageEngine", storageEngine);
        status.setServerStatus(mockServerStatus);
        assertFalse(status.isMajorityReadConcernSupported());

        storageEngine.put("supportsCommittedReads", false);
        status.setServerStatus(mockServerStatus);
        assertFalse(status.isMajorityReadConcernSupported());

        storageEngine.put("supportsCommittedReads", true);
        status.setServerStatus(mockServerStatus);
        assertTrue(status.isMajorityReadConcernSupported());
    }

    @Test
    public void testGetVersion() {
        assertTrue(status.getVersion().matches("^\\d+\\.\\d+\\.\\d+$"));
    }

    @Test
    public void testCheckVersionValid() {
        for (String v : new String[] { "2.6.0", "2.7.0", "3.0.0"}) {
            status.setVersion(v);
            status.checkVersion();
        }
    }

    @Test
    public void testCheckVersionInvalid() {
        for (String v : new String[] { "1.0.0", "2.0.0", "2.5.0"}) {
            status.setVersion(v);
            try {
                status.checkVersion();
                fail("Version " + v + " shouldn't be allowed");
            } catch (Exception e) {
            }
        }
    }
}
