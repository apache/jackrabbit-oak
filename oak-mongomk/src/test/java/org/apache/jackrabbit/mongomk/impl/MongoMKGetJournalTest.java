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

import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for {@code MongoMicroKernel#getJournal(String, String, String)}
 */
public class MongoMKGetJournalTest extends BaseMongoMicroKernelTest {

    @Test
    public void simple() throws Exception {
        String fromDiff = "+\"/a\" : {}";
        String fromMsg = "Add /a";
        String fromRev = mk.commit("", fromDiff, null, fromMsg);

        String toDiff = "+\"/b\" : {}";
        String toMsg = "Add /b";
        String toRev = mk.commit("", toDiff, null, toMsg);

        JSONArray array = parseJSONArray(mk.getJournal(fromRev, toRev, "/"));
        assertEquals(2, array.size());

        JSONObject rev = getObjectArrayEntry(array, 0);
        assertPropertyExists(rev, "id", String.class);
        assertPropertyExists(rev, "ts", Long.class);
        assertPropertyValue(rev, "msg", fromMsg);
        assertPropertyValue(rev, "changes", fromDiff);

        rev = getObjectArrayEntry(array, 1);
        assertPropertyExists(rev, "id", String.class);
        assertPropertyExists(rev, "ts", Long.class);
        assertPropertyValue(rev, "msg", toMsg);
        assertPropertyValue(rev, "changes", toDiff);
    }

    @Test
    @Ignore("OAK-611")
    public void emptyAndRootPath() {
        // Commit with empty path
        String rev = mk.commit("", "+\"/a\":{}", null, "");
        String journalStr = mk.getJournal(rev, rev, "/");
        JSONArray array = parseJSONArray(journalStr);
        JSONObject entry = getObjectArrayEntry(array, 0);
        String expected = "+\"/a\":{}";
        assertPropertyValue(entry, "changes", expected);

        // Commit with root path
        rev = mk.commit("/", "+\"b\":{}", null, "");
        journalStr = mk.getJournal(rev, rev, "/");
        array = parseJSONArray(journalStr);
        entry = getObjectArrayEntry(array, 0);
        expected = "+\"/b\":{}";
        assertPropertyValue(entry, "changes", expected);
    }
}