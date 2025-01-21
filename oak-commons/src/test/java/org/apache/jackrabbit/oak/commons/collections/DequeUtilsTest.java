package org.apache.jackrabbit.oak.commons.collections;


import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for the {@link DequeUtils} class.
 * <p>
 * This class contains test cases to verify the functionality of the methods
 * in the {@link DequeUtils} class.
 */
public class DequeUtilsTest {

    @Test
    public void toArrayDequeWithNonEmptyIterable() {
        List<String> list = Arrays.asList("one", "two", "three");
        ArrayDeque<String> result = DequeUtils.toArrayDeque(list);

        Assert.assertNotNull(result);
        Assert.assertEquals(3, result.size());
        Assert.assertEquals("one", result.peekFirst());
        Assert.assertEquals("three", result.peekLast());
    }

    @Test
    public void toArrayDequeWithEmptyIterable() {
        List<String> emptyList = Collections.emptyList();
        ArrayDeque<String> result = DequeUtils.toArrayDeque(emptyList);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testToArrayDequeWithNullIterable() {
        Assert.assertThrows(NullPointerException.class, () -> DequeUtils.toArrayDeque(null));
    }

}