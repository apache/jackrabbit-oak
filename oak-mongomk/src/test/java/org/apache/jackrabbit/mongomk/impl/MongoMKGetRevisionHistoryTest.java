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

    @Test
    public void path() throws Exception {
        int count1 = 5;
        for (int i = 0; i < count1; i++) {
            mk.commit("/", "+\"a" + i + "\" : {}", null, null);
        }
        JSONArray array = parseJSONArray(mk.getRevisionHistory(0, -1, "/"));
        assertEquals(count1 + 1, array.size());


        int count2 = 5;
        for (int i = 0; i < count2; i++) {
            mk.commit("/a1", "+\"b" + i + "\" : {}", null, null);
        }
        array = parseJSONArray(mk.getRevisionHistory(0, -1, "/"));
        assertEquals(count1 + 1 + count2, array.size());

        array = parseJSONArray(mk.getRevisionHistory(0, -1, "/a1"));
        assertEquals(count2 + 1, array.size());
    }

    @Test
    public void since() throws Exception {
        Thread.sleep(100); // To make sure there's a little delay since the initial commit.
        long since1 = System.currentTimeMillis();
        int count1 = 6;
        for (int i = 0; i < count1; i++) {
            mk.commit("/", "+\"a" + i + "\" : {}", null, null);
        }
        JSONArray array = parseJSONArray(mk.getRevisionHistory(since1, -1, "/"));
        assertEquals(count1, array.size());

        Thread.sleep(100);

        long since2 = System.currentTimeMillis();
        int count2 = 4;
        for (int i = 0; i < count2; i++) {
            mk.commit("/", "+\"b" + i + "\" : {}", null, null);
        }
        array = parseJSONArray(mk.getRevisionHistory(since2, -1, "/"));
        assertEquals(count2, array.size());

        array = parseJSONArray(mk.getRevisionHistory(since1, -1, "/"));
        assertEquals(count1 + count2, array.size());
    }
}