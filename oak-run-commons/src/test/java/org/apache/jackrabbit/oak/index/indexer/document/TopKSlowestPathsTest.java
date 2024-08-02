package org.apache.jackrabbit.oak.index.indexer.document;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class TopKSlowestPathsTest {

    @Test
    public void testSlowestTopKElements() {
        TopKSlowestPaths topK = new TopKSlowestPaths(3);
        topK.add("path4", 400);
        topK.add("path2", 200);
        topK.add("path1", 100);
        topK.add("path3", 300);
        topK.add("path5", 500);
        TopKSlowestPaths.PathAndTime[] entries = topK.getTopK();
        System.out.println(Arrays.toString(entries));
        assertEquals(3, entries.length);
        assertEquals("path5", entries[0].path);
        assertEquals(500, entries[0].timeMillis);
        assertEquals("path4", entries[1].path);
        assertEquals(400, entries[1].timeMillis);
        assertEquals("path3", entries[2].path);
        assertEquals(300, entries[2].timeMillis);
    }

    @Test
    public void testToString() {
        TopKSlowestPaths topK = new TopKSlowestPaths(3);
        topK.add("path4", 400);
        topK.add("path2", 200);
        topK.add("path1", 100);
        topK.add("path3", 300);
        topK.add("path5", 500);
        assertEquals("[path5: 500; path4: 400; path3: 300]", topK.toString());
    }

}