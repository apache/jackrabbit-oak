/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.segment.file;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newConcurrentMap;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

class TarFile {

    /** The tar file block size. */
    private static final int BLOCK_SIZE = 512;

    private static final byte[] ZERO_BYTES = new byte[BLOCK_SIZE];

    private static class Location {

        int offset;

        int size;

        Location(int offset, int size) {
            this.offset = offset;
            this.size = size;
        }

    }

    private final File file;

    private final FileAccess access;

    private int position = 0;

    private final int maxFileSize;

    private final Map<UUID, Location> entries = newConcurrentMap();

    TarFile(File file, int maxFileSize, boolean memoryMapping)
            throws IOException {
        long len = file.length();
        checkState(len <= Integer.MAX_VALUE);
        this.maxFileSize = Math.max((int) len, maxFileSize);

        this.file = file;
        if (memoryMapping) {
            this.access = new MappedAccess(file, this.maxFileSize);
        } else {
            this.access = new RandomAccess(file);
        }

        this.position = 0;
        while (position + BLOCK_SIZE <= len) {
            // read the tar header block
            ByteBuffer buffer = this.access.read(position, BLOCK_SIZE);
            String name = readString(buffer, 100);
            buffer.position(124);
            int size = readNumber(buffer, 12);
            // TODO: verify the checksum, magic, etc.?

            if (name.isEmpty() && size == 0) {
                break; // no more entries in this file
            } else if (position + BLOCK_SIZE + size > len) {
                break; // invalid entry, truncate the file at this point
            }

            try {
                UUID id = UUID.fromString(name);
                entries.put(id, new Location(position + BLOCK_SIZE, size));
            } catch (IllegalArgumentException e) {
                throw new IOException("Unexpected tar entry: " + name);
            }

            position += (1 + (size + BLOCK_SIZE - 1) / BLOCK_SIZE) * BLOCK_SIZE;
        }
    }

    Set<UUID> getUUIDs() {
        return entries.keySet();
    }

    ByteBuffer readEntry(UUID id) throws IOException {
        Location location = entries.get(id);
        if (location != null) {
            return access.read(location.offset, location.size);
        } else {
            return null;
        }
    }

    synchronized boolean writeEntry(UUID id, byte[] b, int offset, int size)
            throws IOException {
        if (position + BLOCK_SIZE + size > maxFileSize) {
            return false;
        }

        byte[] header = new byte[BLOCK_SIZE];

        // File name
        byte[] n = id.toString().getBytes(UTF_8);
        System.arraycopy(n, 0, header, 0, n.length);

        // File mode
        System.arraycopy(
                String.format("%07o", 0400).getBytes(UTF_8), 0,
                header, 100, 7);

        // User's numeric user ID
        System.arraycopy(
                String.format("%07o", 0).getBytes(UTF_8), 0,
                header, 108, 7);

        // Group's numeric user ID
        System.arraycopy(
                String.format("%07o", 0).getBytes(UTF_8), 0,
                header, 116, 7);

        // File size in bytes (octal basis)
        System.arraycopy(
                String.format("%011o", size).getBytes(UTF_8), 0,
                header, 124, 11);

        // Last modification time in numeric Unix time format (octal)
        long time = System.currentTimeMillis() / 1000;
        System.arraycopy(
                String.format("%011o", time).getBytes(UTF_8), 0,
                header, 136, 11);

        // Checksum for header record
        System.arraycopy(
                new byte[] { ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ' }, 0,
                header, 148, 8);

        // Type flag
        header[156] = '0';

        // Compute checksum
        int checksum = 0;
        for (int i = 0; i < header.length; i++) {
            checksum += header[i] & 0xff;
        }
        System.arraycopy(
                String.format("%06o", checksum).getBytes(UTF_8), 0,
                header, 148, 6);
        header[154] = 0;

        access.write(position, header, 0, BLOCK_SIZE);
        position += BLOCK_SIZE;

        access.write(position, b, offset, size);
        entries.put(id, new Location(position, size));
        position += size;

        int padding = BLOCK_SIZE - position % BLOCK_SIZE;
        if (padding < BLOCK_SIZE) {
            access.write(position, ZERO_BYTES, 0, padding);
            position += padding;
        }

        return true;
    }

    public void flush() throws IOException {
        access.flush();
    }


    void close() throws IOException {
        flush();
        access.close();
    }

    private static String readString(ByteBuffer buffer, int fieldSize) {
        byte[] b = new byte[fieldSize];
        buffer.get(b);
        int n = 0;
        while (n < fieldSize && b[n] != 0) {
            n++;
        }
        return new String(b, 0, n, UTF_8);
    }

    private static int readNumber(ByteBuffer buffer, int fieldSize) {
        byte[] b = new byte[fieldSize];
        buffer.get(b);
        int number = 0;
        for (int i = 0; i < fieldSize; i++) {
            int digit = b[i] & 0xff;
            if ('0' <= digit && digit <= '7') {
                number = number * 8 + digit - '0';
            } else {
                break;
            }
        }
        return number;
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        return file.toString();
    }

}
