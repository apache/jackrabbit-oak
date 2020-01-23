package org.apache.jackrabbit.oak.segment.azure;

import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.*;

public class CaseInsensitiveMapAccessTest {

    @Test
    public void convert() {
        Map<String, String> map = CaseInsensitiveMapAccess.convert(Collections.singletonMap("hello", "world"));

        assertEquals("world", map.get("hello"));
        assertEquals("world", map.get("Hello"));
        assertEquals("world", map.get("hELLO"));
    }
}