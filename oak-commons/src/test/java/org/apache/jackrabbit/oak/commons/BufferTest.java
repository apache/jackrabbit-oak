/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.commons;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.BufferOverflowException;
import java.nio.ReadOnlyBufferException;
import java.nio.charset.StandardCharsets;

import org.junit.Test;


/**
 * Tests for {@link Buffer}
 */
public class BufferTest {

    @Test
    public void getFirstCharacterInWrappedBuffer() {
        String testString = "Test string";
        byte[] bytes = testString.getBytes(StandardCharsets.UTF_16BE);
        Buffer buffer = Buffer.wrap(bytes);
        
        // Get the 2 first bytes and convert back to String. Result should be the first 'T' character
        byte byte0 = buffer.get();
        byte byte1 = buffer.get();
        String firstCharacter = new String(new byte[] {byte0, byte1}, StandardCharsets.UTF_16BE);
 
        assertEquals(bytes.length, buffer.capacity());
        assertEquals(2, buffer.position());
        assertEquals(bytes.length - 2, buffer.remaining());
        assertEquals("T", firstCharacter);
        assertEquals(testString, new String(buffer.array(), StandardCharsets.UTF_16BE));
    }
    
    @Test
    public void getFirstCharacterInBoundedWrappedBuffer() {
        String testString = "Test string";
        byte[] bytes = testString.getBytes(StandardCharsets.UTF_16BE);
        Buffer buffer = Buffer.wrap(bytes, 10, 12);
        
        // Get the 2 first bytes and convert back to String. Result should be the 's' character of 'string'
        byte byte0 = buffer.get();
        byte byte1 = buffer.get();
        String firstCharacter = new String(new byte[] {byte0, byte1}, StandardCharsets.UTF_16BE);
 
        assertEquals("s", firstCharacter);
        assertEquals(bytes.length, buffer.capacity());
    }
    
    @Test
    public void getRandomCharacterInWrappedBuffer() {
        String testString = "Test string";
        byte[] bytes = testString.getBytes(StandardCharsets.UTF_16BE);
        Buffer buffer = Buffer.wrap(bytes);
 
        // Get the bytes in positions 16 and 17 and convert back to String. Result should be the 'i' character
        byte byte16 = buffer.get(16);
        byte byte17 = buffer.get(17);
        String iCharacter = new String(new byte[] {byte16, byte17}, StandardCharsets.UTF_16BE);
 
        assertEquals("i", iCharacter);
    }
    
    @Test
    public void getRandomCharactersInWrappedBuffer() {
        String testString = "Test string";
        byte[] bytes = testString.getBytes(StandardCharsets.UTF_8);
        Buffer buffer = Buffer.wrap(bytes);
        
        byte[] result = new byte[8];
        buffer.get(result, 0, 4);
        buffer.rewind();
        buffer.get(result, 4, 4);
        
        assertEquals("TestTest", new String(result));
    }
    
    @Test
    public void resetWrappedBufferToMarkedPosition() {
        String testString = "Test string";
        byte[] bytes = testString.getBytes(StandardCharsets.UTF_16BE);
        Buffer buffer = Buffer.wrap(bytes);
        
        buffer.get();
        buffer.get();
        
        assertEquals(2, buffer.position());
        
        // Mark current position 2
        buffer.mark();
        
        buffer.get();
        buffer.get();
        
        assertEquals(4, buffer.position());
        
        // Reset back to position 2
        buffer.reset();
 
        assertEquals(2, buffer.position());
    }
    
    @Test
    public void putCharacterToWrappedBuffer() {
        String testString = "Test string";
        byte[] bytes = testString.getBytes(StandardCharsets.UTF_8);
        Buffer buffer = Buffer.wrap(bytes);
        
        buffer.position(4);
        
        buffer.put((byte)'-');
        
        assertEquals(5, buffer.position());
        assertEquals("Test-string", new String(buffer.array(), StandardCharsets.UTF_8));
    }
    
    @Test
    public void putStringToWrappedBuffer() {
        Buffer buffer = Buffer.allocate(20);
        
        String testString = "Test";
        byte[] bytes = testString.getBytes(StandardCharsets.UTF_8);
        buffer.put(bytes);
        buffer.put(Buffer.wrap(bytes));
        buffer.flip();
        
        byte[] result = new byte[8];
        buffer.get(result);
        assertEquals(8, buffer.limit());
        assertEquals("TestTest", new String(result, StandardCharsets.UTF_8));
    }
    
    @Test(expected = BufferOverflowException.class)
    public void putStringToLimitedWrappedBuffer() {
        Buffer buffer = Buffer.allocate(20);
        
        buffer.limit(2);
        assertEquals(true, buffer.hasRemaining());
        
        String testString = "Test";
        byte[] bytes = testString.getBytes(StandardCharsets.UTF_8);
        buffer.put(bytes);
    }
    
    @Test
    public void compareWrappedBuffers() {
        String testString1 = "Test string";
        String testString2 = "Test string";
        
        Buffer buffer1 = Buffer.wrap(testString1.getBytes(StandardCharsets.UTF_8));
        Buffer buffer2 = Buffer.wrap(testString2.getBytes(StandardCharsets.UTF_8));
        
        // Both buffers should be equal
        assertTrue(buffer1.equals(buffer2));
        
        // Changing the position make them different
        buffer1.position(2);
        assertFalse(buffer1.equals(buffer2));

        // Restoring the position make them equal again
        buffer1.rewind();
        assertTrue(buffer1.equals(buffer2));
    }
    
    @Test
    public void putNumbersToWrappedBuffer() {
        Buffer buffer = Buffer.allocate(20);
        int intValue = 1000;
        long longValue = 5000000000L;
        
        // Inserting an integer and a long (4 + 8 bytes)
        buffer.putInt(intValue);
        buffer.putLong(longValue);
        
        assertEquals(12, buffer.position());
        
        assertEquals(intValue, buffer.getInt(0));
        assertEquals(longValue, buffer.getLong(4));
        
        buffer.rewind();
        assertEquals(intValue, buffer.getInt());
        assertEquals(longValue, buffer.getLong());
    }
    
    @Test
    public void duplicateWrappedBuffer() {
        String testString = "Test string";
        byte[] byteArray = "-".getBytes(StandardCharsets.UTF_8);
        
        Buffer buffer1 = Buffer.wrap(testString.getBytes(StandardCharsets.UTF_8));
        Buffer buffer2 = buffer1.duplicate();
        
        buffer2.position(4);
        buffer2.put(byteArray, 0, 1);
        
        // Modification of buffer2 should have modified buffer1 too
        assertEquals("Test-string", new String(buffer1.array(), StandardCharsets.UTF_8));
        assertEquals("Test-string", new String(buffer2.array(), StandardCharsets.UTF_8));
    }
    
    @Test(expected = ReadOnlyBufferException.class)
    public void createReadOnlyCopyOfWrappedBuffer() {
        String testString = "Test string";
        byte[] byteArray = "-".getBytes(StandardCharsets.UTF_8);
        
        Buffer buffer1 = Buffer.wrap(testString.getBytes(StandardCharsets.UTF_8));
        Buffer buffer2 = buffer1.asReadOnlyBuffer();
        
        buffer2.position(4);
        // Exception thrown here, as the copy is read-only
        buffer2.put(byteArray, 0, 1);
    }
}
