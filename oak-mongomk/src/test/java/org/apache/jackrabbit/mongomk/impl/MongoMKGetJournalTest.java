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
import org.junit.Test;

/**
 * Tests for {@code MongoMicroKernel#getJournal(String, String, String)}
 */
public class MongoMKGetJournalTest extends BaseMongoMicroKernelTest {

    @Test
    public void simple() throws Exception {
        String fromDiff = "+\"/a\":{}";
        String fromMsg = "Add /a";
        String fromRev = mk.commit("", fromDiff, null, fromMsg);

        String toDiff = "+\"/b\":{}";
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
    public void commitAddWithDiffPaths() {
        // Commit with empty path and retrieve with root path
        String rev = mk.commit("", "+\"/a\":{}", null, "");
        String journalStr = mk.getJournal(rev, rev, "/");
        JSONArray array = parseJSONArray(journalStr);
        JSONObject entry = getObjectArrayEntry(array, 0);
        String expected = "+\"/a\":{}";
        assertPropertyValue(entry, "changes", expected);

        // Commit with root path and retrieve with root path
        rev = mk.commit("/", "+\"b\":{}", null, "");
        journalStr = mk.getJournal(rev, rev, "/");
        array = parseJSONArray(journalStr);
        entry = getObjectArrayEntry(array, 0);
        expected = "+\"/b\":{}";
        assertPropertyValue(entry, "changes", expected);

        // Commit with /b path and retrieve with root path
        rev = mk.commit("/b", "+\"c\":{}", null, "");
        journalStr = mk.getJournal(rev, rev, "/");
        array = parseJSONArray(journalStr);
        entry = getObjectArrayEntry(array, 0);
        expected = "+\"/b/c\":{}";
        assertPropertyValue(entry, "changes", expected);

        // Commit with /b/c path and retrieve with root path
        rev = mk.commit("/b/c", "+\"d\":{}", null, "");
        journalStr = mk.getJournal(rev, rev, "/");
        array = parseJSONArray(journalStr);
        entry = getObjectArrayEntry(array, 0);
        expected = "+\"/b/c/d\":{}";
        assertPropertyValue(entry, "changes", expected);
    }

    @Test
    public void commitCopyWithDiffPaths() {
        mk.commit("", "+\"/a\":{}", null, "");

        // Commit with empty path and retrieve with root path
        String rev = mk.commit("", "*\"/a\" : \"/b\"", null, null);
        String journalStr = mk.getJournal(rev, rev, "/");
        JSONArray array = parseJSONArray(journalStr);
        JSONObject entry = getObjectArrayEntry(array, 0);
        String expected = "*\"/a\":\"/b\"";
        assertPropertyValue(entry, "changes", expected);

        // Commit with root path and retrieve with root path
        rev = mk.commit("/", "*\"b\" : \"c\"", null, null);
        journalStr = mk.getJournal(rev, rev, "/");
        array = parseJSONArray(journalStr);
        entry = getObjectArrayEntry(array, 0);
        expected = "*\"/b\":\"/c\"";
        assertPropertyValue(entry, "changes", expected);

        mk.commit("", "+\"/b/d\":{}", null, "");

        // Commit with /b path and retrieve with root path
        rev = mk.commit("/b", "*\"d\" : \"e\"", null, null);
        journalStr = mk.getJournal(rev, rev, "/");
        array = parseJSONArray(journalStr);
        entry = getObjectArrayEntry(array, 0);
        expected = "*\"/b/d\":\"/b/e\"";
        assertPropertyValue(entry, "changes", expected);
    }

    @Test
    public void commitMoveWithDiffPaths() {
        mk.commit("", "+\"/a\":{}", null, "");

        // Commit with empty path and retrieve with root path
        String rev = mk.commit("", ">\"/a\" : \"/b\"", null, null);
        String journalStr = mk.getJournal(rev, rev, "/");
        JSONArray array = parseJSONArray(journalStr);
        JSONObject entry = getObjectArrayEntry(array, 0);
        String expected = ">\"/a\":\"/b\"";
        assertPropertyValue(entry, "changes", expected);

        // Commit with root path and retrieve with root path
        rev = mk.commit("/", ">\"b\" : \"c\"", null, null);
        journalStr = mk.getJournal(rev, rev, "/");
        array = parseJSONArray(journalStr);
        entry = getObjectArrayEntry(array, 0);
        expected = ">\"/b\":\"/c\"";
        assertPropertyValue(entry, "changes", expected);

        mk.commit("", "+\"/d\":{}", null, "");
        mk.commit("", "+\"/d/e\":{}", null, "");

        // Commit with /d path and retrieve with root path
        rev = mk.commit("/d", ">\"e\" : \"f\"", null, null);
        journalStr = mk.getJournal(rev, rev, "/");
        array = parseJSONArray(journalStr);
        entry = getObjectArrayEntry(array, 0);
        expected = ">\"/d/e\":\"/d/f\"";
        assertPropertyValue(entry, "changes", expected);
    }

    @Test
    public void commitRemoveMoveWithDiffPaths() {
        mk.commit("", "+\"/a\":{}", null, "");

        // Commit with empty path and retrieve with root path
        String rev = mk.commit("", "-\"/a\"", null, null);
        String journalStr = mk.getJournal(rev, rev, "/");
        JSONArray array = parseJSONArray(journalStr);
        JSONObject entry = getObjectArrayEntry(array, 0);
        String expected = "-\"/a\"";
        assertPropertyValue(entry, "changes", expected);

        mk.commit("", "+\"/b\":{}", null, "");

        // Commit with root path and retrieve with root path
        rev = mk.commit("", "-\"/b\"", null, null);
        journalStr = mk.getJournal(rev, rev, "/");
        array = parseJSONArray(journalStr);
        entry = getObjectArrayEntry(array, 0);
        expected = "-\"/b\"";
        assertPropertyValue(entry, "changes", expected);

        mk.commit("", "+\"/b\":{\"c\" : {}}", null, "");

        // Commit with /b path and retrieve with root path
        rev = mk.commit("/b", "-\"c\"", null, null);
        journalStr = mk.getJournal(rev, rev, "/");
        array = parseJSONArray(journalStr);
        entry = getObjectArrayEntry(array, 0);
        expected = "-\"/b/c\"";
        assertPropertyValue(entry, "changes", expected);
    }

    @Test
    public void commitSetPropertyWithDiffPaths() {
        mk.commit("", "+\"/a\":{}", null, "");

        // Commit with empty path and retrieve with root path
        String rev = mk.commit("", "^\"/a/key1\" : \"value1\"", null, null);
        String journalStr = mk.getJournal(rev, rev, "/");
        JSONArray array = parseJSONArray(journalStr);
        JSONObject entry = getObjectArrayEntry(array, 0);
        String expected = "^\"/a/key1\":\"value1\"";
        assertPropertyValue(entry, "changes", expected);

        // Commit with root path and retrieve with root path
        rev = mk.commit("/", "^\"a/key2\" : \"value2\"", null, null);
        journalStr = mk.getJournal(rev, rev, "/");
        array = parseJSONArray(journalStr);
        entry = getObjectArrayEntry(array, 0);
        expected = "^\"/a/key2\":\"value2\"";
        assertPropertyValue(entry, "changes", expected);

        // Commit with /a path and retrieve with root path
        rev = mk.commit("/a", "^\"key3\": \"value3\"", null, "");
        journalStr = mk.getJournal(rev, rev, "/");
        array = parseJSONArray(journalStr);
        entry = getObjectArrayEntry(array, 0);
        expected = "^\"/a/key3\":\"value3\"";
        assertPropertyValue(entry, "changes", expected);
    }
}