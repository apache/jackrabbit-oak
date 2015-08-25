package org.apache.jackrabbit.oak.plugins.blob.migration;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

public class DfsNodeIteratorTest {

    private NodeStore store;

    @Before
    public void setup() throws CommitFailedException {
        store = SegmentNodeStore.newSegmentNodeStore(new MemoryStore()).create();
        final NodeBuilder rootBuilder = store.getRoot().builder();
        final NodeBuilder countries = rootBuilder.child("countries");
        countries.child("uk").child("cities").child("london").child("districts").child("frognal");
        countries.child("germany");
        countries.child("france").child("cities").child("paris");
        store.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    // The order of the returned nodes is not defined, that's why we have to
    // create 3 subtrees.
    @Test
    public void testIterate() {
        final Map<String, String[]> subtrees = new HashMap<String, String[]>();
        subtrees.put("uk", new String[] { "cities", "london", "districts", "frognal" });
        subtrees.put("germany", new String[] {});
        subtrees.put("france", new String[] { "cities", "paris" });

        final DfsNodeIterator iterator = new DfsNodeIterator(store.getRoot());
        assertTrue(iterator.hasNext());
        assertEquals("countries", iterator.next().getName());

        for (int i = 0; i < 3; i++) {
            assertTrue(iterator.hasNext());
            final String country = iterator.next().getName();
            for (String node : subtrees.remove(country)) {
                assertTrue(iterator.hasNext());
                assertEquals(node, iterator.next().getName());
            }
        }
        assertFalse(iterator.hasNext());
        assertTrue(subtrees.isEmpty());
    }

    @Test
    public void testGetPath() {
        final Map<String, String> nameToPath = new HashMap<String, String>();
        nameToPath.put("countries", "/countries");
        nameToPath.put("uk", "/countries/uk");
        nameToPath.put("frognal", "/countries/uk/cities/london/districts/frognal");
        nameToPath.put("paris", "/countries/france/cities/paris");

        final DfsNodeIterator iterator = new DfsNodeIterator(store.getRoot());
        while (iterator.hasNext()) {
            final String expectedPath = nameToPath.remove(iterator.next().getName());
            if (expectedPath == null) {
                continue;
            }
            assertEquals(expectedPath, iterator.getPath());
        }
        assertTrue(nameToPath.isEmpty());
    }

    @Test
    public void testFastForward() {
        DfsNodeIterator iterator = new DfsNodeIterator(store.getRoot(), "/countries/uk");
        assertTrue(iterator.hasNext());
        assertEquals("cities", iterator.next().getName());
        assertTrue(iterator.hasNext());
        assertEquals("london", iterator.next().getName());

        iterator = new DfsNodeIterator(store.getRoot(), "/");
        assertTrue(iterator.hasNext());
        assertEquals("countries", iterator.next().getName());
    }
}
