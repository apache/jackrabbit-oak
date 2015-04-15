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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.h2.mvstore.FileStore;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVMap.Builder;
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
    private static final AtomicInteger COUNTER = new AtomicInteger();
    
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
    private MapFactory writeStore;
    private MapFactory readStore;
    private int maxSizeMB = 1024;
    private int readGeneration = -1;
    private int writeGeneration;
    private long maxBinaryEntry = 1024 * 1024;
    private int autoCompact = 50;
    private boolean appendOnly;

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
                dir += "-" + System.currentTimeMillis() + "-" + COUNTER.getAndIncrement();
            } else if (p.startsWith("size=")) {
                maxSizeMB = Integer.parseInt(p.split("=")[1]);
            } else if (p.startsWith("binary=")) {
                maxBinaryEntry = Long.parseLong(p.split("=")[1]);
            } else if (p.startsWith("autoCompact=")) {
                autoCompact = Integer.parseInt(p.split("=")[1]);
            } else if (p.equals("appendOnly")) {
                appendOnly = true;
            }
        }
        this.directory = dir;
        if (dir.length() == 0) {
            readGeneration = -1;
            writeGeneration = 0;
            writeStore = createMapFactory(writeGeneration, false);
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
            for (File f : list) {
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
            readStore = createMapFactory(readGeneration, true);
        }
        writeStore = createMapFactory(writeGeneration, false);
    }
    
    private String getFileName(int generation) {
        if (directory.length() == 0) {
            return null;
        }
        return directory + "/" + FILE_PREFIX + generation + FILE_SUFFIX;
    }
    
    private MapFactory createMapFactory(final int generation, final boolean readOnly) {
        MapFactory f = new MapFactory() {
            
            final String fileName = getFileName(generation);
            MVStore store;
            
            @Override
            void openStore() {
                if (store != null) {
                    return;
                }
                MVStore.Builder builder = new MVStore.Builder();
                try {
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
                    if (autoCompact >= 0) {
                        builder.autoCompactFillRate(autoCompact);
                    }
                    builder.backgroundExceptionHandler(new Thread.UncaughtExceptionHandler() {
                        @Override
                        public void uncaughtException(Thread t, Throwable e) {
                            LOG.debug("Error in the background thread of the persistent cache", e);
                            LOG.warn("Error in the background thread of the persistent cache: " + e);
                        }
                    });
                    store = builder.open();
                    if (appendOnly) {
                        store.setReuseSpace(false);
                    }
                } catch (Exception e) {
                    LOG.warn("Could not open the store " + fileName, e);
                }
            }
            
            @Override
            synchronized void closeStore() {
                if (store == null) {
                    return;
                }
                try {
                    boolean compact = compactOnClose;
                    if (store.getFileStore().isReadOnly()) {
                        compact = false;
                    }
                    store.close();
                    if (compact) {
                        MVStoreTool.compact(fileName, true);
                    }
                } catch (Exception e) {
                    LOG.debug("Could not close or compact the store", e);
                    LOG.warn("Could not close or compact the store: " + e);
                }
                store = null;
            }

            @Override
            <K, V> Map<K, V> openMap(String name, Builder<K, V> builder) {
                try {
                    if (builder == null) {
                        return store.openMap(name);
                    }
                    return store.openMap(name, builder);
                } catch (Exception e) {
                    LOG.warn("Could not open the map", e);
                    return null;
                }
            }

            @Override
            long getFileSize() {
                try {
                    FileStore fs = store.getFileStore();
                    if (fs == null) {
                        return 0;
                    }
                    return fs.size();
                } catch (Exception e) {
                    LOG.warn("Could not retrieve the map size", e);
                    return 0;
                }
            }
        };
        f.openStore();
        return f;
    }
    
    public void close() {
        if (writeStore != null) {
            writeStore.closeStore();
        }
        if (readStore != null) {
            readStore.closeStore();
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
        case CONSOLIDATED_DIFF:
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
    
    public synchronized <K, V> CacheMap<K, V> openMap(int generation, String name, 
            MVMap.Builder<K, V> builder) {
        MapFactory s;
        if (generation == readGeneration) {
            s = readStore;
        } else if (generation == writeGeneration) {
            s = writeStore;
        } else {
            throw new IllegalArgumentException("Unknown generation: " + generation);
        }
        return new CacheMap<K, V>(s, name, builder);
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
            MapFactory oldRead = readStore;
            readStore = writeStore;
            readGeneration = writeGeneration;
            MapFactory w = createMapFactory(writeGeneration + 1, false);
            writeStore = w;
            writeGeneration++;
            for (GenerationCache c : caches) {
                c.addGeneration(writeGeneration, false);
                if (oldReadGeneration >= 0) {
                    c.removeGeneration(oldReadGeneration);
                }
            }
            if (oldRead != null) {
                oldRead.closeStore();
                new File(getFileName(oldReadGeneration)).delete();
            }
        }
    }
    
    private boolean needSwitch() {
        long size = writeStore.getFileSize();
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
    
    public int getOpenCount() {
        return writeStore.getOpenCount();
    }


    interface GenerationCache {

        void addGeneration(int writeGeneration, boolean b);

        void removeGeneration(int oldReadGeneration);
        
    }

}
