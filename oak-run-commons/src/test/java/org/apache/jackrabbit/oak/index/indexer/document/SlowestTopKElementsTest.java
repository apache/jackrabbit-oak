package org.apache.jackrabbit.oak.index.indexer.document;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class SlowestTopKElementsTest {

    @Test
    public void testSlowestTopKElements() {
        SlowestTopKElements slowestTopKElements = new SlowestTopKElements(3);
        slowestTopKElements.add("path4", 400);
        slowestTopKElements.add("path2", 200);
        slowestTopKElements.add("path1", 100);
        slowestTopKElements.add("path3", 300);
        slowestTopKElements.add("path5", 500);
        SlowestTopKElements.Entry[] entries = slowestTopKElements.getTopK();
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
        SlowestTopKElements slowestTopKElements = new SlowestTopKElements(3);
        slowestTopKElements.add("path4", 400);
        slowestTopKElements.add("path2", 200);
        slowestTopKElements.add("path1", 100);
        slowestTopKElements.add("path3", 300);
        slowestTopKElements.add("path5", 500);
        assertEquals("[path5: 500; path4: 400; path3: 300]", slowestTopKElements.toString());
    }

}