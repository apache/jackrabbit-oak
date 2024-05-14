package org.apache.jackrabbit.oak.plugins.memory;

import junit.framework.TestCase;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

public class StringPropertyStateTest extends TestCase {


    @Test
    public void testGetValue() throws IOException {
        String value = Arrays.toString(Files.readAllBytes(Paths.get("src/test/resources/sample.txt")));
        StringPropertyState stringPropertyState = new StringPropertyState("name", value);
        assertEquals(value, stringPropertyState.getValue());
    }

}