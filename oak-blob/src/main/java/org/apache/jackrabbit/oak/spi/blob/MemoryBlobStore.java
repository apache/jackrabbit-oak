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
package org.apache.jackrabbit.oak.spi.blob;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Maps;

import org.apache.jackrabbit.oak.commons.StringUtils;

/**
 * A memory blob store. Useful for testing.
 */
public class MemoryBlobStore extends AbstractBlobStore {

    private HashMap<BlockId, byte[]> map = new HashMap<BlockId, byte[]>();
    private HashMap<BlockId, byte[]> old = new HashMap<BlockId, byte[]>();
    private boolean mark;

    @Override
    protected byte[] readBlockFromBackend(BlockId id) {
        byte[] result = map.get(id);
        if (result == null) {
            result = old.get(id);
        }
        return result;
    }

    @Override
    protected synchronized void storeBlock(byte[] digest, int level, byte[] data) {
        map.put(new BlockId(digest, 0), data);
    }

    @Override
    public void startMark() throws IOException {
        mark = true;
        old = map;
        map = new HashMap<BlockId, byte[]>();
        markInUse();
    }

    @Override
    protected boolean isMarkEnabled() {
        return mark;
    }

    @Override
    protected void mark(BlockId id) {
        byte[] data = map.get(id);
        if (data == null) {
            data = old.get(id);
            if (data != null) {
                map.put(id, data);
            }
        }
    }

    @Override
    public int sweep() {
        int count = old.size();
        old.clear();
        mark = false;
        return count;
    }

    /**
     * Ignores the maxlastModifiedTime
     */
    @Override
    public long countDeleteChunks(List<String> chunkIds, long maxLastModifiedTime) throws Exception {
        int count = 0;
        for (String chunkId : chunkIds) {
            BlockId id = new BlockId(StringUtils.convertHexToBytes(chunkId), 0);
            if (map.containsKey(id)) {
                map.remove(id);
                count++;
            } else if (old.containsKey(id)) {
                old.remove(id);
                count++;
            }
        }
        return count;
    }

    /**
     * Ignores the maxlastModifiedTime
     */
    @Override
    public Iterator<String> getAllChunkIds(long maxLastModifiedTime) throws Exception {
        HashMap<BlockId, byte[]> combinedMap = Maps.newHashMap();
        combinedMap.putAll(map);
        combinedMap.putAll(old);
        final Iterator<BlockId> iter = combinedMap.keySet().iterator();

        return new AbstractIterator<String>() {
            @Override
            protected String computeNext() {
                if (iter.hasNext()) {
                    BlockId blockId = iter.next();
                    if (blockId != null) {
                        return StringUtils.convertBytesToHex(blockId.getDigest());
                    }
                }
                return endOfData();
            }
        };
    }
    
    @Override
    public void clearCache() {
        // no cache
    }

}
