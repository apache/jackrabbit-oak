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
package org.apache.jackrabbit.oak.segment;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.util.Objects.requireNonNull;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A disk-backed (memory mapped) hash map from a fixed-width byte key to a
 * {@link RecordId}, used by {@link RecordRepacker} to keep the alias table and the
 * content-deduplication cache off the Java heap. The mapping is stored in a temporary
 * file which is deleted when the map is {@link #close() closed}, so the entries do not
 * count against the heap and can grow well beyond it.
 * <p>
 * The table is an open-addressing hash table with linear probing. Each slot holds a
 * one-byte occupancy flag, the {@code keyWidth}-byte key and the 20-byte serialised
 * value ({@code msb, lsb, recordNumber}). Values are reconstructed through a
 * {@link SegmentIdProvider}, which interns the (few) target-generation segment ids.
 * Entries are never removed; the table grows (and rehashes into a fresh file) once the
 * load factor is exceeded.
 * <p>
 * This is a prototype helper: it is single-threaded, keeps at most one live mapping and
 * therefore caps a single table at 2&nbsp;GB. That is ample for repacking the stores this
 * prototype targets; a larger table would need a paged mapping.
 */
final class SpillableRecordIdMap implements Closeable {

    private static final int VALUE_WIDTH = 20; // msb (8) + lsb (8) + recordNumber (4)

    private static final float LOAD_FACTOR = 0.6f;

    @NotNull
    private final SegmentIdProvider idProvider;

    private final int keyWidth;

    private final int slotWidth;

    /** Largest power-of-two capacity whose byte size still fits a single {@code int}-addressed mapping. */
    private final int maxCapacity;

    private final String name;

    private Path file;

    private MappedByteBuffer buffer;

    private int capacity;

    private long size;

    /** Reusable scratch for the serialised key of the {@link RecordId} key overloads. */
    private final byte[] keyScratch;

    SpillableRecordIdMap(@NotNull SegmentIdProvider idProvider, int keyWidth, int initialCapacity,
            @NotNull String name) throws IOException {
        this.idProvider = requireNonNull(idProvider);
        this.keyWidth = keyWidth;
        this.slotWidth = 1 + keyWidth + VALUE_WIDTH;
        this.maxCapacity = Integer.highestOneBit(Integer.MAX_VALUE / slotWidth);
        this.name = requireNonNull(name);
        this.keyScratch = new byte[keyWidth];
        this.capacity = Math.min(tableSize(initialCapacity), maxCapacity);
        this.file = Files.createTempFile("oak-repack-" + name + "-", ".map");
        this.file.toFile().deleteOnExit();
        this.buffer = mapFile(file, (long) capacity * slotWidth);
    }

    // -- RecordId key overloads (alias table) --------------------------------

    @Nullable
    RecordId get(@NotNull RecordId key) {
        serializeId(key, keyScratch);
        return get(keyScratch);
    }

    void put(@NotNull RecordId key, @NotNull RecordId value) throws IOException {
        serializeId(key, keyScratch);
        put(keyScratch, value);
    }

    // -- byte[] key overloads (dedup cache) ----------------------------------

    @Nullable
    RecordId get(@NotNull byte[] key) {
        int index = slotFor(key, capacity);
        while (true) {
            int pos = index * slotWidth;
            if (buffer.get(pos) == 0) {
                return null;
            }
            if (keyEquals(pos, key)) {
                return readValue(pos);
            }
            index = (index + 1) & (capacity - 1);
        }
    }

    void put(@NotNull byte[] key, @NotNull RecordId value) throws IOException {
        if (size + 1 > (long) (capacity * LOAD_FACTOR)) {
            grow();
        }
        insert(buffer, capacity, key, value.getSegmentId().getMostSignificantBits(),
                value.getSegmentId().getLeastSignificantBits(), value.getRecordNumber());
    }

    long size() {
        return size;
    }

    @Override
    public void close() {
        unmap(buffer);
        buffer = null;
        if (file != null) {
            file.toFile().delete();
            file = null;
        }
    }

    // -- internals -----------------------------------------------------------

    /**
     * Insert into {@code buf} (of {@code cap} slots), incrementing {@link #size} only when a
     * new key is added. An existing key is overwritten in place.
     */
    private void insert(MappedByteBuffer buf, int cap, byte[] key, long msb, long lsb, int recordNumber) {
        int index = slotFor(key, cap);
        while (true) {
            int pos = index * slotWidth;
            if (buf.get(pos) == 0) {
                buf.put(pos, (byte) 1);
                int base = pos + 1;
                for (int i = 0; i < keyWidth; i++) {
                    buf.put(base + i, key[i]);
                }
                int vbase = base + keyWidth;
                buf.putLong(vbase, msb);
                buf.putLong(vbase + 8, lsb);
                buf.putInt(vbase + 16, recordNumber);
                size++;
                return;
            }
            if (bufKeyEquals(buf, pos, key)) {
                int vbase = pos + 1 + keyWidth;
                buf.putLong(vbase, msb);
                buf.putLong(vbase + 8, lsb);
                buf.putInt(vbase + 16, recordNumber);
                return;
            }
            index = (index + 1) & (cap - 1);
        }
    }

    private void grow() throws IOException {
        if (capacity >= maxCapacity) {
            throw new IllegalStateException("Spillable map '" + name + "' exceeded the maximum size of "
                    + ((long) maxCapacity * slotWidth) + " bytes");
        }
        int newCapacity = capacity * 2;
        Path newFile = Files.createTempFile("oak-repack-" + name + "-", ".map");
        newFile.toFile().deleteOnExit();
        MappedByteBuffer newBuffer = mapFile(newFile, (long) newCapacity * slotWidth);

        byte[] key = new byte[keyWidth];
        long moved = size;
        size = 0;
        for (int index = 0; index < capacity; index++) {
            int pos = index * slotWidth;
            if (buffer.get(pos) != 0) {
                int base = pos + 1;
                for (int i = 0; i < keyWidth; i++) {
                    key[i] = buffer.get(base + i);
                }
                int vbase = base + keyWidth;
                insert(newBuffer, newCapacity, key, buffer.getLong(vbase),
                        buffer.getLong(vbase + 8), buffer.getInt(vbase + 16));
            }
        }
        assert size == moved : "rehash lost entries";

        unmap(buffer);
        file.toFile().delete();

        this.buffer = newBuffer;
        this.file = newFile;
        this.capacity = newCapacity;
    }

    private int slotFor(byte[] key, int cap) {
        return (int) (hash(key) & (cap - 1));
    }

    private boolean keyEquals(int pos, byte[] key) {
        return bufKeyEquals(buffer, pos, key);
    }

    private boolean bufKeyEquals(MappedByteBuffer buf, int pos, byte[] key) {
        int base = pos + 1;
        for (int i = 0; i < keyWidth; i++) {
            if (buf.get(base + i) != key[i]) {
                return false;
            }
        }
        return true;
    }

    private RecordId readValue(int pos) {
        int vbase = pos + 1 + keyWidth;
        long msb = buffer.getLong(vbase);
        long lsb = buffer.getLong(vbase + 8);
        int recordNumber = buffer.getInt(vbase + 16);
        return new RecordId(idProvider.newSegmentId(msb, lsb), recordNumber);
    }

    private static void serializeId(RecordId id, byte[] out) {
        SegmentId segmentId = id.getSegmentId();
        writeLong(out, 0, segmentId.getMostSignificantBits());
        writeLong(out, 8, segmentId.getLeastSignificantBits());
        writeInt(out, 16, id.getRecordNumber());
    }

    private static void writeLong(byte[] out, int offset, long value) {
        for (int i = 0; i < 8; i++) {
            out[offset + i] = (byte) (value >>> (56 - 8 * i));
        }
    }

    private static void writeInt(byte[] out, int offset, int value) {
        for (int i = 0; i < 4; i++) {
            out[offset + i] = (byte) (value >>> (24 - 8 * i));
        }
    }

    /** FNV-1a over the key bytes with a final avalanche mix. */
    private long hash(byte[] key) {
        long h = 0xcbf29ce484222325L;
        for (int i = 0; i < keyWidth; i++) {
            h ^= (key[i] & 0xffL);
            h *= 0x100000001b3L;
        }
        h ^= (h >>> 32);
        return h;
    }

    private static int tableSize(int expectedEntries) {
        int min = Math.max(16, expectedEntries);
        int size = Integer.highestOneBit(min - 1) << 1;
        return size < 16 ? 16 : size;
    }

    private static MappedByteBuffer mapFile(Path file, long bytes) throws IOException {
        // The mapping outlives the channel/descriptor, so the backing file is only opened long
        // enough to size and map it. This keeps the descriptor count low even with many shards.
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.setLength(bytes);
            return raf.getChannel().map(READ_WRITE, 0, bytes);
        }
    }

    /** Best-effort unmap so the (deleted) file's pages are released promptly rather than at GC. */
    private static void unmap(MappedByteBuffer buffer) {
        if (buffer == null) {
            return;
        }
        try {
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            Field theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            Object unsafe = theUnsafe.get(null);
            Method invokeCleaner = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
            invokeCleaner.invoke(unsafe, buffer);
        } catch (Throwable t) {
            // The mapping will be released when the buffer is garbage collected.
        }
    }
}
