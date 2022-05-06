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

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Junits for {@link MongoVersion}
 *
 * @author daim
 */
public class MongoVersionTest {


    @Test
    public void createMongoVersion() {
        MongoVersion version = MongoVersion.of("4.0.0");
        assertEquals(MongoVersion.MONGO_4_0_0, version);
    }

    @Test
    public void createDefaultMongoVersionFromNull() {
        MongoVersion version = MongoVersion.of(null);
        assertEquals(MongoVersion.MONGO_4_0_0, version);
    }

    @Test
    public void createDefaultMongoVersionMalformed() {
        MongoVersion version = MongoVersion.of("234");
        assertEquals(MongoVersion.MONGO_4_0_0, version);
    }

    @Test
    public void createDefaultMongoVersionMalformed2() {
        MongoVersion version = MongoVersion.of("4.0");
        assertEquals(MongoVersion.MONGO_4_0_0, version);
    }

    @Test
    public void compareMongoVersion() {
        MongoVersion version = MongoVersion.of("2.3.4");
        assertTrue(MongoVersion.MONGO_4_0_0.compareTo(version) > 0);
    }

    @Test
    public void compareMongoVersion2() {
        MongoVersion version = MongoVersion.of("4.0.4");
        assertTrue(MongoVersion.MONGO_4_0_0.compareTo(version) < 0);
    }

}