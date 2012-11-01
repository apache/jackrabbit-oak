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
package org.apache.jackrabbit.mongomk.impl;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.json.simple.JSONObject;
import org.junit.Test;

/**
 * Tests for {@code MongoMicroKernel#getHeadRevision()}.
 */
public class MongoMKGetNodesTest extends BaseMongoMicroKernelTest {

    @Test
    public void invalidRevision() throws Exception {
        try {
            mk.getNodes("/", "invalid", 1, 0, -1, null);
            fail("Exception expected");
        } catch (Exception expected) {}
    }

    @Test
    public void afterDelete() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mk);
        String revisionId = scenario.create();
        JSONObject root = parseJSONObject(mk.getNodes("/", revisionId,1, 0, -1, null));
        assertPropertyValue(root, ":childNodeCount", 1L);
        JSONObject a = resolveObjectValue(root, "a");
        assertNotNull(a);
        assertPropertyValue(a, ":childNodeCount", 2L);

        revisionId = scenario.delete_A();
        root = parseJSONObject(mk.getNodes("/", revisionId,1, 0, -1, null));
        assertPropertyValue(root, ":childNodeCount", 0L);
    }
}