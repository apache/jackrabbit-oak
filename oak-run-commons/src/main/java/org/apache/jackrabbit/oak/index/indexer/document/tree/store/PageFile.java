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
package org.apache.jackrabbit.oak.index.indexer.document.tree.store;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils.MemoryObject;

/**
 * A B-tree page (leaf, or inner node).
 * An inner node contains one more value than keys.
 * A leaf page has the same number of keys and values.
 */
public class PageFile implements MemoryObject {

    private static final boolean VERIFY_SIZE = false;
    private static final int INITIAL_SIZE_IN_BYTES = 24;

    private String fileName;
    private final long maxFileSizeBytes;

    private final boolean innerNode;

    private static ByteBuffer REUSED_BUFFER = ByteBuffer.allocate(1024 * 1024);

    private ArrayList<String> keys = new ArrayList<>();
    private ArrayList<String> values = new ArrayList<>();
    private long update;
    private String nextRoot;
    private int sizeInBytes = INITIAL_SIZE_IN_BYTES;

    // -1: beginning; 0: middle; 1: end
    private int lastSearchIndex;

    // contains unwritten modifications
    private boolean modified;

    public PageFile(boolean innerNode, long maxFileSizeBytes) {
        this.innerNode = innerNode;
        this.maxFileSizeBytes = maxFileSizeBytes;
    }

    @Override
    public long estimatedMemory() {
        return maxFileSizeBytes;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }

    public void setUpdate(long update) {
        modified = true;
        this.update = update;
    }

    public static PageFile fromBytes(byte[] data, long maxFileSizeBytes) {
        ByteBuffer buff = ByteBuffer.wrap(data);
        int type = buff.get();
        String nextRoot = readString(buff);
        long update = buff.getLong();
        String prefix = readString(buff);
        int len = buff.getInt();
        PageFile result;
        if (type == 0) {
            result = new PageFile(true, maxFileSizeBytes);
            for (int i = 0; i < len; i++) {
                result.appendRecord(prefix + readString(buff), readString(buff));
            }
            result.values.add(readString(buff));
        } else {
            result = new PageFile(false, maxFileSizeBytes);
            for (int i = 0; i < len; i++) {
                result.appendRecord(prefix + readString(buff), readString(buff));
            }
        }
        if (!nextRoot.isEmpty()) {
            result.setNextRoot(nextRoot);
        }
        result.setUpdate(update);
        result.modified = false;
        return result;
    }

    public byte[] toBytes() {
        // synchronization is needed because we share the buffer
        synchronized (PageFile.class) {
            ByteBuffer buff = REUSED_BUFFER;
            if (buff.capacity() < sizeInBytes * 2) {
                buff = REUSED_BUFFER = ByteBuffer.allocate(sizeInBytes * 2);
            }
            buff.rewind();
            // first byte may not be '4', as that is used for LZ4 compression
            buff.put((byte) (innerNode ? 0 : 1));
            writeString(buff, nextRoot == null ? "" : nextRoot);
            buff.putLong(update);
            String prefix = keys.size() < 2 ? "" : commonPrefix(keys.get(0), keys.get(keys.size() - 1));
            writeString(buff, prefix);
            buff.putInt(keys.size());
            if (innerNode) {
                for (int i = 0; i < keys.size(); i++) {
                    writeString(buff, keys.get(i).substring(prefix.length()));
                    writeString(buff, values.get(i));
                }
                writeString(buff, values.get(values.size() - 1));
            } else {
                for (int i = 0; i < keys.size(); i++) {
                    writeString(buff, keys.get(i).substring(prefix.length()));
                    writeString(buff, values.get(i));
                }
            }
            buff.flip();
            buff.rewind();
            byte[] array = new byte[buff.remaining()];
            buff.get(array);
            // reset the limit
            REUSED_BUFFER = ByteBuffer.wrap(buff.array());
            return array;
        }
    }

    private void writeString(ByteBuffer buff, String s) {
        if (s == null) {
            buff.putShort((short) -2);
            return;
        }
        byte[] data = s.getBytes(StandardCharsets.UTF_8);
        if (data.length < Short.MAX_VALUE) {
            // could get a bit larger, but some negative values are reserved
            buff.putShort((short) data.length);
        } else {
            buff.putShort((short) -1);
            buff.putInt(data.length);
        }
        buff.put(data);
    }

    private static String readString(ByteBuffer buff) {
        int len = buff.getShort();
        if (len == -2) {
            return null;
        } else if (len == -1) {
            len = buff.getInt();
            int pos = buff.position();
            buff.position(buff.position() + len);
            return new String(buff.array(), pos + buff.arrayOffset(), len, StandardCharsets.UTF_8);
        } else {
            int pos = buff.position();
            buff.position(buff.position() + len);
            return new String(buff.array(), pos + buff.arrayOffset(), len, StandardCharsets.UTF_8);
        }
    }

    private static String commonPrefix(String prefix, String x) {
        if (prefix == null) {
            return x;
        }
        int i = 0;
        for (; i < prefix.length() && i < x.length(); i++) {
            if (prefix.charAt(i) != x.charAt(i)) {
                break;
            }
        }
        return prefix.substring(0, i);
    }

    public String toString() {
        return keys + "" + values;
    }

    public PageFile copy() {
        PageFile result = new PageFile(innerNode, maxFileSizeBytes);
        result.modified = modified;
        result.keys = new ArrayList<>(keys);
        result.values = new ArrayList<>(values);
        result.sizeInBytes = sizeInBytes;
        result.nextRoot = nextRoot;
        return result;
    }

    public void addChild(int index, String childKey, String newChildFileName) {
        modified = true;
        if (index > 0) {
            keys.add(index - 1, childKey);
            sizeInBytes += childKey.length();
        }
        values.add(index, newChildFileName);
        sizeInBytes += 4;
        sizeInBytes += newChildFileName.length();
    }

    public void setValue(int index, String value) {
        modified = true;
        sizeInBytes -= sizeInBytes(values.get(index));
        sizeInBytes += sizeInBytes(value);
        values.set(index, value);
    }

    private long sizeInBytes(String obj) {
        if (obj == null) {
            return 5;
        } else if (obj instanceof String) {
            return ((String) obj).length() + 2;
        } else {
            throw new IllegalStateException();
        }
    }

    public void removeRecord(int index) {
        modified = true;
        String key = keys.remove(index);
        String value = values.remove(index);
        sizeInBytes -= 4;
        sizeInBytes -= key.length();
        sizeInBytes -= sizeInBytes(value);
    }

    public void appendRecord(String k, String v) {
        modified = true;
        keys.add(k);
        values.add(v);
        sizeInBytes += 4;
        sizeInBytes += k.length();
        sizeInBytes += sizeInBytes(v);
    }

    public void insertRecord(int index, String key, String value) {
        modified = true;
        keys.add(index, key);
        values.add(index, value);
        sizeInBytes += 4;
        sizeInBytes += key.length();
        sizeInBytes += sizeInBytes(value);
    }

    public long getUpdate() {
        return update;
    }

    public int sizeInBytes() {
        if (VERIFY_SIZE) {
            int size = 24;
            for (String p : keys) {
                size += p.length();
                size += 4;
            }
            for (String o : values) {
                size += sizeInBytes(o);
            }
            if (size != sizeInBytes) {
                throw new AssertionError();
            }
        }
        return sizeInBytes;
    }

    public boolean canSplit() {
        if (innerNode) {
            return keys.size() > 2;
        } else {
            return keys.size() > 1;
        }
    }

    public List<String> getKeys() {
        return keys;
    }

    public int getKeyIndex(String key) {
        int index;
        if (keys.isEmpty()) {
            return -1;
        }
        if (lastSearchIndex == 1) {
            if (key.compareTo(keys.get(keys.size() - 1)) > 0) {
                index = -(keys.size() + 1);
            } else {
                index = Collections.binarySearch(keys, key);
            }
        } else if (lastSearchIndex == -1) {
            if (key.compareTo(keys.get(0)) < 0) {
                index = -1;
            } else {
                index = Collections.binarySearch(keys, key);
            }
        } else {
            index = Collections.binarySearch(keys, key);
        }
        if (index == -(keys.size() + 1)) {
            lastSearchIndex = 1;
        } else if (index == -1) {
            lastSearchIndex = -1;
        } else {
            lastSearchIndex = 0;
        }
        return index;
    }

    public String getValue(int index) {
        return values.get(index);
    }

    public String getChildValue(int index) {
        return (String) values.get(index);
    }

    public String getNextKey(String largerThan) {
        int index;
        if (largerThan == null) {
            index = 0;
        } else {
            index = getKeyIndex(largerThan);
            if (index < 0) {
                index = -index - 1;
            } else {
                index++;
            }
        }
        if (index < 0 || index >= keys.size()) {
            return null;
        }
        return keys.get(index);
    }

    public String getNextRoot() {
        return nextRoot;
    }

    public void setNextRoot(String nextRoot) {
        modified = true;
        this.nextRoot = nextRoot;
    }

    public void removeKey(int index) {
        modified = true;
        String key = keys.get(index);
        sizeInBytes -= key.length();
        sizeInBytes -= 4;
        keys.remove(index);
    }

    public void removeValue(int index) {
        modified = true;
        String x = (String) values.get(index);
        sizeInBytes -= x.length();
        values.remove(index);
    }

    public boolean isInnerNode() {
        return innerNode;
    }

    public int getValueCount() {
        return values.size();
    }

    public String getKey(int index) {
        return keys.get(index);
    }

    public void setModified(boolean modified) {
        this.modified = modified;
    }

    public boolean isModified() {
        return modified;
    }

}
