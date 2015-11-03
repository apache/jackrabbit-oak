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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class MongoMissingLastRevSeekerTest {

    private MongoConnection c;
    private String dbName;
    private DocumentMK.Builder builder;
    private DocumentNodeStore ns;

    @Before
    public void before() throws Exception {
        c = MongoUtils.getConnection();
        assumeTrue(c != null);
        dbName = c.getDB().getName();
        builder = new DocumentMK.Builder().setMongoDB(c.getDB());
        ns = builder.getNodeStore();
    }

    @After
    public void after() throws Exception {
        if (ns != null) {
            ns.dispose();
        }
        if (c != null) {
            c.close();
        }
        MongoUtils.dropDatabase(dbName);
    }

    @Test
    public void missingLastRevSeeker() throws Exception {
        assertTrue(builder.createMissingLastRevSeeker() instanceof MongoMissingLastRevSeeker);
    }
}
