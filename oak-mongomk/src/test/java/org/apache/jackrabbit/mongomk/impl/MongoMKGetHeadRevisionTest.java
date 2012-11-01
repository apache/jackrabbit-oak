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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.junit.Test;

/**
 * Tests for {@code MongoMicroKernel#getHeadRevision()}.
 */
public class MongoMKGetHeadRevisionTest extends BaseMongoMicroKernelTest {

    @Test
    public void simple() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mk);
        String rev1 = scenario.create();

        String rev2 = mk.getHeadRevision();
        assertEquals(rev1, rev2);

        String rev3 = scenario.delete_A();
        assertFalse(rev3 == rev2);
    }
}