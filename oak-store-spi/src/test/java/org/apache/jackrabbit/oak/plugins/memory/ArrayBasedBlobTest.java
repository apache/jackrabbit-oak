package org.apache.jackrabbit.oak.plugins.memory;

import junit.framework.TestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ArrayBasedBlobTest extends TestCase {


    public void testGetNewStream() throws IOException {
        byte[] fileContent = Files.readAllBytes(Paths.get("src/test/resources/sample.txt"));
        ArrayBasedBlob arrayBasedBlob = new ArrayBasedBlob(fileContent);
        String expected = new String(fileContent);
        assertEquals(expected, new String(arrayBasedBlob.getNewStream().readAllBytes()));
        assertEquals(fileContent.length, arrayBasedBlob.length());
    }

}