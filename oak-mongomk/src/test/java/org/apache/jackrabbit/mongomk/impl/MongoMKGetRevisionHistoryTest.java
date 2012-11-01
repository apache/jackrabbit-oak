package org.apache.jackrabbit.mongomk.impl;

import static org.junit.Assert.assertEquals;

import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.json.simple.JSONArray;
import org.junit.Test;

/**
 * Tests for {@link MongoMicroKernel#getRevisionHistory(long, int, String)}
 */
public class MongoMKGetRevisionHistoryTest extends BaseMongoMicroKernelTest {

    @Test
    public void maxEntriesZero() throws Exception {
        mk.commit("/", "+\"a\" : {}", null, null);
        JSONArray array = parseJSONArray(mk.getRevisionHistory(0, 0, "/"));
        assertEquals(0, array.size());
    }

    @Test
    public void maxEntriesLimitless() throws Exception {
        int count = 10;
        for (int i = 0; i < count; i++) {
            mk.commit("/", "+\"a" + i + "\" : {}", null, null);
        }
        JSONArray array = parseJSONArray(mk.getRevisionHistory(0, -1, "/"));
        assertEquals(count + 1, array.size());
    }

    @Test
    public void maxEntriesLimited() throws Exception {
        int count = 10;
        int limit = 4;

        for (int i = 0; i < count; i++) {
            mk.commit("/", "+\"a" + i + "\" : {}", null, null);
        }
        JSONArray array = parseJSONArray(mk.getRevisionHistory(0, limit, "/"));
        assertEquals(limit, array.size());
    }
}