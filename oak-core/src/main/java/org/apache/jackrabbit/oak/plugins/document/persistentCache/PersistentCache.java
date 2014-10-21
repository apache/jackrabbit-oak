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
package org.apache.jackrabbit.oak.plugins.document.persistentCache;

import java.io.File;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeSet;

import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.h2.mvstore.FileStore;
import org.h2.mvstore.MVMapConcurrent;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.MVStoreTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;

/**
 * A persistent cache for the document store.
 */
public class PersistentCache {
    
    static final Logger LOG = LoggerFactory.getLogger(PersistentCache.class);
   
    private static final String FILE_PREFIX = "cache-";
    private static final String FILE_SUFFIX = ".data";
    
    private boolean cacheNodes = true;
    private boolean cacheChildren = true;
    private boolean cacheDiff = true;
    private boolean cacheDocs;
    private boolean cacheDocChildren;
    private boolean compactOnClose = true;
    private boolean compress = true;
    private ArrayList<GenerationCache> caches = 
            new ArrayList<GenerationCache>();
    
    private final String directory;
    private MVStore writeStore;
    private MVStore readStore;
    private int maxSizeMB = 1024;
    private int readGeneration = -1;
    private int writeGeneration = 0;
    private long maxBinaryEntry = 1024 * 1024;
    
    public PersistentCache(String url) {
        LOG.info("start version 1");
        String[] parts = url.split(",");
        String dir = parts[0];
        for (String p : parts) {
            if (p.equals("+docs")) {
                cacheDocs = true;
            } else if (p.equals("+docChildren")) {
                cacheDocChildren = true;
            } else if (p.equals("-nodes")) {
                cacheNodes = false;
            } else if (p.equals("-children")) {
                cacheChildren = false;
            } else if (p.equals("-diff")) {
                cacheDiff = false;
            } else if (p.equals("+all")) {
                cacheDocs = true;
                cacheDocChildren = true;
            } else if (p.equals("-compact")) {
                compactOnClose = false;
            } else if (p.equals("-compress")) {
                compress = false;
            } else if (p.endsWith("time")) {
                dir += "-" + System.currentTimeMillis();
            } else if (p.startsWith("size=")) {
                maxSizeMB = Integer.parseInt(p.split("=")[1]);
            } else if (p.startsWith("binary=")) {
                maxBinaryEntry = Long.parseLong(p.split("=")[1]);
            }
        }
        this.directory = dir;
        if (dir.length() == 0) {
            readGeneration = -1;
            writeGeneration = 0;
            writeStore = openStore(writeGeneration, false);
            return;
        }
        File dr = new File(dir);
        if (!dr.exists()) {
            dr.mkdirs();
        }
        if (dr.exists() && !dr.isDirectory()) {
            throw new IllegalArgumentException("A file exists at cache directory " + dir);
        }
        File[] list = dr.listFiles();
        TreeSet<Integer> generations = new TreeSet<Integer>();
        if (list != null) {
            for(File f : list) {
                String fn = f.getName();
                if (fn.startsWith(FILE_PREFIX) && fn.endsWith(FILE_SUFFIX)) {
                    String g = fn.substring(FILE_PREFIX.length(), fn.indexOf(FILE_SUFFIX));
                    try {
                        int gen = Integer.parseInt(g);
                        if (gen >= 0) {
                            File f2 = new File(getFileName(gen));
                            if (fn.equals(f2.getName())) {
                                // ignore things like "cache-000.data"
                                generations.add(gen);
                            }
                        }
                    } catch (Exception e) {
                        // ignore this file
                    }
                }
            }
        }
        while (generations.size() > 2) {
            generations.remove(generations.last());
        }
        readGeneration = generations.size() > 1 ? generations.first() : -1;
        writeGeneration = generations.size() > 0 ? generations.last() : 0;
        if (readGeneration >= 0) {
            readStore = openStore(readGeneration, true);
        }
        writeStore = openStore(writeGeneration, false);
    }
    
    private String getFileName(int generation) {
        if (directory.length() == 0) {
            return null;
        }
        return directory + "/" + FILE_PREFIX + generation + FILE_SUFFIX;
    }
    
    private MVStore openStore(int generation, boolean readOnly) {
        String fileName = getFileName(generation);
        MVStore.Builder builder = new MVStore.Builder();
        if (compress) {
            builder.compress();
        }
        if (fileName != null) {
            builder.fileName(fileName);
        }
        if (readOnly) {
            builder.readOnly();
        }
        if (maxSizeMB < 10) {
            builder.cacheSize(maxSizeMB);
        }
        builder.backgroundExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                LOG.error("Error in persistent cache", e);
            }
        });
        return builder.open();
    }
    
    public void close() {
        closeStore(writeStore, writeGeneration);
        closeStore(readStore, readGeneration);
    }
    
    private void closeStore(MVStore s, int generation) {
        if (s == null) {
            return;
        }
        String fileName = getFileName(generation);
        boolean compact = compactOnClose;
        if (s.getFileStore().isReadOnly()) {
            compact = false;
        }
        s.close();
        if (compact) {
            MVStoreTool.compact(fileName, true);
        }
    }
    
    public synchronized GarbageCollectableBlobStore wrapBlobStore(
            GarbageCollectableBlobStore base) {
        BlobCache c = new BlobCache(this, base);
        initGenerationCache(c);
        return c;
    }
    
    public synchronized <K, V> Cache<K, V> wrap(
            DocumentNodeStore docNodeStore, 
            DocumentStore docStore,
            Cache<K, V> base, CacheType type) {
        boolean wrap;
        switch (type) {
        case NODE:
            wrap = cacheNodes;
            break;
        case CHILDREN:
            wrap = cacheChildren;
            break;
        case DIFF:
            wrap = cacheDiff;
            break;
        case DOC_CHILDREN:
            wrap = cacheDocChildren;
            break;
        case DOCUMENT:
            wrap = cacheDocs;
            break;
        default:  
            wrap = false;
            break;
        }
        if (wrap) {
            NodeCache<K, V> c = new NodeCache<K, V>(this, base, docNodeStore, docStore, type);
            initGenerationCache(c);
            return c;
        }
        return base;
    }
    
    private void initGenerationCache(GenerationCache c) {
        caches.add(c);
        if (readGeneration >= 0) {
            c.addGeneration(readGeneration, true);
        }
        c.addGeneration(writeGeneration, false);
    }
    
    synchronized <K, V> Map<K, V> openMap(int generation, String name, 
            MVMapConcurrent.Builder<K, V> builder) {
        MVStore s;
        if (generation == readGeneration) {
            s = readStore;
        } else if (generation == writeGeneration) {
            s = writeStore;
        } else {
            throw new IllegalArgumentException("Unknown generation: " + generation);
        }
        return s.openMap(name, builder);
    }
    
    public void switchGenerationIfNeeded() {
        if (!needSwitch()) {
            return;
        }
        synchronized (this) {
            // maybe another thread already switched,
            // so we need to check again
            if (!needSwitch()) {
                return;
            }
            int oldReadGeneration = readGeneration;
            MVStore oldRead = readStore;
            readStore = writeStore;
            readGeneration = writeGeneration;
            MVStore w = openStore(writeGeneration + 1, false);
            writeStore = w;
            writeGeneration++;
            for (GenerationCache c : caches) {
                c.addGeneration(writeGeneration, false);
                if (oldReadGeneration >= 0) {
                    c.removeGeneration(oldReadGeneration);
                }
            }
            if (oldRead != null) {
                oldRead.close();
                new File(getFileName(oldReadGeneration)).delete();
            }
        }
    }
    
    private boolean needSwitch() {
        FileStore fs = writeStore.getFileStore();
        if (fs == null) {
            return false;
        }
        long size = fs.size();
        if (size / 1024 / 1024 <= maxSizeMB) {
            return false;
        }
        return true;
    }
    
    public int getMaxSize() {
        return maxSizeMB;
    }
    
    public long getMaxBinaryEntrySize() {
        return maxBinaryEntry;
    }

    static interface GenerationCache {

        void addGeneration(int writeGeneration, boolean b);

        void removeGeneration(int oldReadGeneration);
        
        
    }

}
