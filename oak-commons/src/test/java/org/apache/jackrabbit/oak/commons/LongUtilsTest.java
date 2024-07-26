package org.apache.jackrabbit.oak.commons;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class LongUtilsTest {

    @Test
    public void tryParse() {
        assertEquals(-1, LongUtils.tryParse("-1").longValue());
        assertEquals(0, LongUtils.tryParse("0").longValue());
        assertEquals(1, LongUtils.tryParse("1").longValue());
        assertEquals(Long.MAX_VALUE, LongUtils.tryParse(String.valueOf(Long.MAX_VALUE)).longValue());
        assertEquals(Long.MIN_VALUE, LongUtils.tryParse(String.valueOf(Long.MIN_VALUE)).longValue());
        assertEquals(Integer.MIN_VALUE, LongUtils.tryParse(String.valueOf(Integer.MIN_VALUE)).longValue());
        assertEquals(Integer.MAX_VALUE, LongUtils.tryParse(String.valueOf(Integer.MAX_VALUE)).longValue());
        assertNull(LongUtils.tryParse("0.1"));
        assertNull(LongUtils.tryParse("1.1"));
        assertNull(LongUtils.tryParse("-1.1"));
        assertNull(LongUtils.tryParse("1.1e3"));
        assertNull(LongUtils.tryParse("foo"));
    }
}