package org.apache.jackrabbit.oak.segment.azure.util;

import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class CaseInsensitiveKeysMapAccessTest {

    @Test
    public void convert() {
        Map<String, String> map = CaseInsensitiveKeysMapAccess.convert(Collections.singletonMap("hello", "world"));

        assertEquals("world", map.get("hello"));
        assertEquals("world", map.get("Hello"));
        assertEquals("world", map.get("hELLO"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void assertImmutable() {
        Map<String, String> map = CaseInsensitiveKeysMapAccess.convert(Collections.singletonMap("hello", "world"));
        map.put("foo", "bar");
    }
}
