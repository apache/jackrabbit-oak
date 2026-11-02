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

import com.mongodb.client.MongoClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests the minimum server version enforced by {@link MongoStatus} without MongoDB.
 */
public class MongoStatusVersionTest {

    private final MongoClient client = Mockito.mock(MongoClient.class);
    private final MongoStatus status = new MongoStatus(client, "test");

    /**
     * Version checks use the supplied version without contacting MongoDB.
     */
    @After
    public void verifyNoDatabaseAccess() {
        Mockito.verifyNoInteractions(client);
    }

    /**
     * The first supported server release is accepted.
     */
    @Test
    public void acceptsMinimumVersion() {
        status.setVersion("4.0.0");
        status.checkVersion();
    }

    /**
     * Patch releases and newer server generations remain supported.
     */
    @Test
    public void acceptsNewerVersions() {
        for (String version : new String[] {"4.0.28", "4.2.0", "4.2.24", "4.4.0", "5.0.0", "8.0.0"}) {
            status.setVersion(version);
            status.checkVersion();
        }
    }

    /**
     * Older servers are rejected with the required and actual versions.
     */
    @Test
    public void rejectsUnsupportedVersions() {
        for (String version : new String[] {"3.6.23", "3.9.99", "2.6.0", "2.5.0", "2.0.0", "1.0.0"}) {
            status.setVersion(version);
            RuntimeException exception = Assert.assertThrows("Version " + version,
                    RuntimeException.class, status::checkVersion);
            Assert.assertEquals("MongoDB version 4.0.0 or higher required. "
                    + "Currently connected to a MongoDB with version: " + version,
                    exception.getMessage());
        }
    }
}
