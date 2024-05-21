/*
 * COPIED FROM APACHE LUCENE 4.7.2
 *
 * Git URL: git@github.com:apache/lucene.git, tag: releases/lucene-solr/4.7.2, path: lucene/core/src/java
 *
 * (see https://issues.apache.org/jira/browse/OAK-10786 for details)
 */

package org.apache.lucene.store;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.util.IOUtils;

/**
 * Abstract base class for performing read operations of Lucene's low-level data types.
 *
 * <p>{@code DataInput} may only be used from one thread, because it is not
 * thread safe (it keeps internal state like file position). To allow multithreaded use, every
 * {@code DataInput} instance must be cloned before used in another thread. Subclasses must
 * therefore implement {@link #clone()}, returning a new {@code DataInput} which operates on the
 * same underlying resource, but positioned independently.
 */
public abstract class DataInput implements Cloneable {

    /**
     * Reads and returns a single byte.
     *
     * @see DataOutput#writeByte(byte)
     */
    public abstract byte readByte() throws IOException;

    /**
     * Reads a specified number of bytes into an array at the specified offset.
     *
     * @param b      the array to read bytes into
     * @param offset the offset in the array to start storing bytes
     * @param len    the number of bytes to read
     * @see DataOutput#writeBytes(byte[], int)
     */
    public abstract void readBytes(byte[] b, int offset, int len)
        throws IOException;

    /**
     * Reads a specified number of bytes into an array at the specified offset with control over
     * whether the read should be buffered (callers who have their own buffer should pass in "false"
     * for useBuffer).  Currently only {@link BufferedIndexInput} respects this parameter.
     *
     * @param b         the array to read bytes into
     * @param offset    the offset in the array to start storing bytes
     * @param len       the number of bytes to read
     * @param useBuffer set to false if the caller will handle buffering.
     * @see DataOutput#writeBytes(byte[], int)
     */
    public void readBytes(byte[] b, int offset, int len, boolean useBuffer)
        throws IOException {
        // Default to ignoring useBuffer entirely
        readBytes(b, offset, len);
    }

    /**
     * Reads two bytes and returns a short.
     *
     * @see DataOutput#writeByte(byte)
     */
    public short readShort() throws IOException {
        return (short) (((readByte() & 0xFF) << 8) | (readByte() & 0xFF));
    }

    /**
     * Reads four bytes and returns an int.
     *
     * @see DataOutput#writeInt(int)
     */
    public int readInt() throws IOException {
        return ((readByte() & 0xFF) << 24) | ((readByte() & 0xFF) << 16)
            | ((readByte() & 0xFF) << 8) | (readByte() & 0xFF);
    }

    /**
     * Reads an int stored in variable-length format.  Reads between one and five bytes.  Smaller
     * values take fewer bytes.  Negative numbers are not supported.
     * <p>
     * The format is described further in {@link DataOutput#writeVInt(int)}.
     *
     * @see DataOutput#writeVInt(int)
     */
    public int readVInt() throws IOException {
    /* This is the original code of this method,
     * but a Hotspot bug (see LUCENE-2975) corrupts the for-loop if
     * readByte() is inlined. So the loop was unwinded!
    byte b = readByte();
    int i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
      b = readByte();
      i |= (b & 0x7F) << shift;
    }
    return i;
    */
        byte b = readByte();
        if (b >= 0) {
            return b;
        }
        int i = b & 0x7F;
        b = readByte();
        i |= (b & 0x7F) << 7;
        if (b >= 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7F) << 14;
        if (b >= 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7F) << 21;
        if (b >= 0) {
            return i;
        }
        b = readByte();
        // Warning: the next ands use 0x0F / 0xF0 - beware copy/paste errors:
        i |= (b & 0x0F) << 28;
        if ((b & 0xF0) == 0) {
            return i;
        }
        throw new IOException("Invalid vInt detected (too many bits)");
    }

    /**
     * Reads eight bytes and returns a long.
     *
     * @see DataOutput#writeLong(long)
     */
    public long readLong() throws IOException {
        return (((long) readInt()) << 32) | (readInt() & 0xFFFFFFFFL);
    }

    /**
     * Reads a long stored in variable-length format.  Reads between one and nine bytes.  Smaller
     * values take fewer bytes.  Negative numbers are not supported.
     * <p>
     * The format is described further in {@link DataOutput#writeVInt(int)}.
     *
     * @see DataOutput#writeVLong(long)
     */
    public long readVLong() throws IOException {
    /* This is the original code of this method,
     * but a Hotspot bug (see LUCENE-2975) corrupts the for-loop if
     * readByte() is inlined. So the loop was unwinded!
    byte b = readByte();
    long i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
      b = readByte();
      i |= (b & 0x7FL) << shift;
    }
    return i;
    */
        byte b = readByte();
        if (b >= 0) {
            return b;
        }
        long i = b & 0x7FL;
        b = readByte();
        i |= (b & 0x7FL) << 7;
        if (b >= 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 14;
        if (b >= 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 21;
        if (b >= 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 28;
        if (b >= 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 35;
        if (b >= 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 42;
        if (b >= 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 49;
        if (b >= 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 56;
        if (b >= 0) {
            return i;
        }
        throw new IOException("Invalid vLong detected (negative values disallowed)");
    }

    /**
     * Reads a string.
     *
     * @see DataOutput#writeString(String)
     */
    public String readString() throws IOException {
        int length = readVInt();
        final byte[] bytes = new byte[length];
        readBytes(bytes, 0, length);
        return new String(bytes, 0, length, IOUtils.CHARSET_UTF_8);
    }

    /**
     * Returns a clone of this stream.
     *
     * <p>Clones of a stream access the same data, and are positioned at the same
     * point as the stream they were cloned from.
     *
     * <p>Expert: Subclasses must ensure that clones may be positioned at
     * different points in the input from each other and from the stream they were cloned from.
     */
    @Override
    public DataInput clone() {
        try {
            return (DataInput) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new Error("This cannot happen: Failing to clone DataInput");
        }
    }

    /**
     * Reads a Map&lt;String,String&gt; previously written with
     * {@link DataOutput#writeStringStringMap(Map)}.
     */
    public Map<String, String> readStringStringMap() throws IOException {
        final Map<String, String> map = new HashMap<String, String>();
        final int count = readInt();
        for (int i = 0; i < count; i++) {
            final String key = readString();
            final String val = readString();
            map.put(key, val);
        }

        return map;
    }

    /**
     * Reads a Set&lt;String&gt; previously written with {@link DataOutput#writeStringSet(Set)}.
     */
    public Set<String> readStringSet() throws IOException {
        final Set<String> set = new HashSet<String>();
        final int count = readInt();
        for (int i = 0; i < count; i++) {
            set.add(readString());
        }

        return set;
    }
}
