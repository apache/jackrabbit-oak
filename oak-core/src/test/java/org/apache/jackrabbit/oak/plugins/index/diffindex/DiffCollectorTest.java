package org.apache.jackrabbit.oak.plugins.index.diffindex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Set;

import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class DiffCollectorTest {

    @Test
    public void testUUID() throws Exception {
        NodeState root = MemoryNodeState.EMPTY_NODE;

        NodeBuilder builder = root.builder();
        builder.child("a").setProperty("jcr:uuid", "abc");
        builder.child("b").setProperty("jcr:uuid", "xyz");

        NodeState after = builder.getNodeState();

        UUIDDiffCollector collector = new UUIDDiffCollector(root, after);

        FilterImpl f = new FilterImpl(null, null);
        f.restrictProperty("jcr:uuid", Operator.EQUAL,
                PropertyValues.newString("abc"));

        Set<String> result = collector.getResults(f);
        Iterator<String> iterator = result.iterator();
        assertTrue(iterator.hasNext());
        assertEquals("a", iterator.next());
        assertFalse(iterator.hasNext());

    }

    @Test
    public void testUUIDInner() throws Exception {
        NodeState root = MemoryNodeState.EMPTY_NODE;
        NodeBuilder builder = root.builder();

        builder.child("a").setProperty("jcr:uuid", "abc");
        NodeState before = builder.getNodeState();

        builder = before.builder();
        builder.child("a").child("b").setProperty("jcr:uuid", "xyz");
        NodeState after = builder.getNodeState();

        UUIDDiffCollector collector = new UUIDDiffCollector(before, after);

        FilterImpl f = new FilterImpl(null, null);
        f.restrictProperty("jcr:uuid", Operator.EQUAL,
                PropertyValues.newString("xyz"));

        Set<String> result = collector.getResults(f);
        Iterator<String> iterator = result.iterator();
        assertTrue(iterator.hasNext());
        assertEquals("a/b", iterator.next());
        assertFalse(iterator.hasNext());
    }

}
