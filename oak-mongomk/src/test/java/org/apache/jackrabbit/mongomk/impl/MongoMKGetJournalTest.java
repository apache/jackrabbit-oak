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
        String fromDiff = "+\"a\" : {}";
        String fromMsg = "Add /a";
        String fromRev = mk.commit("/", fromDiff, null, fromMsg);

        String toDiff = "+\"b\" : {}";
        String toMsg = "Add /b";
        String toRev = mk.commit("/", toDiff, null, toMsg);

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

}