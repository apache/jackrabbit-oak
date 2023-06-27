package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.Assert.*;

public class BoundedHistogramTest {

    @Test
    public void testOverflow() {
        int maxSize = 100;
        BoundedHistogram histogram = new BoundedHistogram("test", maxSize);
        // Add enough entries to overflow the histogram
        for (int i = 0; i < maxSize * 2; i++) {
            histogram.addEntry("/a/b/c/path" + i);
        }
        // Another pass. The entries that are already in the histogram should be incremented
        for (int i = 0; i < maxSize * 2; i++) {
            histogram.addEntry("/a/b/c/path" + i);
        }
        // Check that the size was capped at MAX_HISTOGRAM_SIZE
        ConcurrentHashMap<String, LongAdder> map = histogram.getMap();
        assertEquals(maxSize, map.size());
        map.values().forEach((v) -> assertEquals(2, v.intValue()));
    }
}