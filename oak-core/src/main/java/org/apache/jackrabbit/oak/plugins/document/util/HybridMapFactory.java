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
package org.apache.jackrabbit.oak.plugins.document.util;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ForwardingConcurrentMap;
import com.google.common.collect.Maps;

import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A hybrid MapFactory implementation switching from an in-memory map to a
 * MapDB backed map when the size of the map reaches a threshold. Once the
 * map shrinks again, the implementation switches back to an in-memory map.
 *
 * This factory implementation keeps track of maps created by {@link #create()}
 * and {@link #create(Comparator)} with weak references and disposes the
 * underlying {@link MapDBMapFactory} when a map created by that factory is not
 * referenceable and not in use anymore.
 */
public class HybridMapFactory extends MapFactory {

    private static final Logger LOG = LoggerFactory.getLogger(HybridMapFactory.class);

    /**
     * Keep at most this number of entries in memory, otherwise use a map
     * implementation backed by MapDB.
     */
    static final int IN_MEMORY_SIZE_LIMIT = 100000;

    /**
     * Switch to an in-memory map when there are less than 10'000 entries
     * in the map.
     */
    static final int IN_MEMORY_SIZE_LIMIT_LOW = 10000;

    /**
     * Reference queue for maps backed by MapDB.
     */
    private final ReferenceQueue<Object> queue = new ReferenceQueue<Object>();

    /**
     * Maps {@link MapReference}s to their corresponding MapFactory.
     */
    private final Map<Reference, MapFactory> factories = Maps.newIdentityHashMap();

    @Override
    public ConcurrentMap<String, Revision> create() {
        return create(null);
    }

    @Override
    public ConcurrentMap<String, Revision> create(Comparator<String> comparator) {
        return new MapImpl(comparator);
    }

    @Override
    public void dispose() {
        for (MapFactory f : factories.values()) {
            dispose(f);
        }
        factories.clear();
    }

    //------------------------------< internal >--------------------------------

    private synchronized void pollReferenceQueue() {
        Reference ref;
        while ((ref = queue.poll()) != null) {
            dispose(factories.remove(ref));
        }
    }

    private void dispose(MapFactory factory) {
        try {
            if (factory != null) {
                Stopwatch sw = Stopwatch.createStarted();
                factory.dispose();
                sw.stop();
                LOG.debug("Disposed MapDB map in {}", sw);
            }
        } catch (Exception e) {
            LOG.warn("Failed to dispose MapFactory", e);
        }
    }

    /**
     * A map implementation, which forwards calls to either an in-memory or
     * MapDB backed map implementation. Methods, which modify the underlying map
     * are synchronized because of OAK-2888.
     */
    private final class MapImpl extends
            ForwardingConcurrentMap<String, Revision> {

        /**
         * A comparator, if keys of the map are sorted, otherwise {@code null}.
         */
        private final Comparator<String> comparator;

        /**
         * The current map. This is either in-memory or a map backed by MapDB.
         */
        private volatile ConcurrentMap<String, Revision> map;

        /**
         * Maintain size of {@link #map} because the in-memory MapFactory
         * variant uses a ConcurrentSkipListMap with a non-constant size() cost.
         */
        private long size;

        /**
         * Whether the current {@link #map} is in memory.
         */
        private boolean mapInMemory;

        MapImpl(@Nullable Comparator<String> comparator) {
            this.comparator = comparator;
            this.map = createMap(true);
            this.mapInMemory = true;
        }

        @Override
        protected ConcurrentMap<String, Revision> delegate() {
            return map;
        }

        @Override
        public synchronized Revision putIfAbsent(@Nonnull String key,
                                                 @Nonnull Revision value) {
            Revision r = map.putIfAbsent(key, checkNotNull(value));
            if (r == null) {
                size++;
                maybeSwitchToMapDB();
            }
            return r;
        }

        @Override
        public synchronized Revision put(@Nonnull String key, @Nonnull Revision value) {
            Revision r = map.put(key, checkNotNull(value));
            if (r == null) {
                size++;
                maybeSwitchToMapDB();
            }
            return r;
        }

        @Override
        public synchronized void putAll(@Nonnull Map<? extends String, ? extends Revision> map) {
            for (Map.Entry<? extends String, ? extends Revision> entry : map.entrySet()) {
                if (this.map.put(entry.getKey(), checkNotNull(entry.getValue())) == null) {
                    size++;
                    maybeSwitchToMapDB();
                }
            }
        }

        @Override
        public synchronized boolean remove(@Nonnull Object key,
                                           @Nonnull Object value) {
            boolean remove = map.remove(key, value);
            if (remove) {
                size--;
                maybeSwitchToInMemoryMap();
            }
            return remove;
        }

        @Override
        public synchronized Revision remove(@Nonnull Object object) {
            Revision r = map.remove(object);
            if (r != null) {
                size--;
                maybeSwitchToInMemoryMap();
            }
            return r;
        }

        @Override
        public synchronized Revision replace(@Nonnull String key,
                                             @Nonnull Revision value) {
            return map.replace(key, checkNotNull(value));
        }

        @Override
        public synchronized boolean replace(@Nonnull String key,
                                            @Nonnull Revision oldValue,
                                            @Nonnull Revision newValue) {
            return map.replace(key, checkNotNull(oldValue), checkNotNull(newValue));
        }

        @Override
        public synchronized void clear() {
            map.clear();
            size = 0;
        }

        @Nonnull
        @Override
        public Collection<Revision> values() {
            return Collections.unmodifiableCollection(map.values());
        }

        @Nonnull
        @Override
        public Set<Entry<String, Revision>> entrySet() {
            return Collections.unmodifiableSet(map.entrySet());
        }

        @Nonnull
        @Override
        public Set<String> keySet() {
            return Collections.unmodifiableSet(map.keySet());
        }

        private void maybeSwitchToMapDB() {
            // switch map if
            // - map is currently in-memory
            // - size limit is reached
            if (mapInMemory
                    && size >= IN_MEMORY_SIZE_LIMIT) {
                Stopwatch sw = Stopwatch.createStarted();
                ConcurrentMap<String, Revision> tmp = createMap(false);
                tmp.putAll(map);
                map = tmp;
                mapInMemory = false;
                sw.stop();
                LOG.debug("Switched to MapDB map in {}", sw);
                pollReferenceQueue();
            }
        }

        private void maybeSwitchToInMemoryMap() {
            // only switch to in memory if not already in memory
            // and size is less than lower limit
            if (!mapInMemory && size < IN_MEMORY_SIZE_LIMIT_LOW) {
                Stopwatch sw = Stopwatch.createStarted();
                ConcurrentMap<String, Revision> tmp = createMap(true);
                tmp.putAll(map);
                map = tmp;
                mapInMemory = true;
                sw.stop();
                LOG.debug("Switched to in-memory map in {}", sw);
                pollReferenceQueue();
            }
        }

        private ConcurrentMap<String, Revision> createMap(boolean inMemory) {
            MapFactory f;
            if (inMemory) {
                f = MapFactory.DEFAULT;
            } else {
                f = new MapDBMapFactory();
            }
            ConcurrentMap<String, Revision> map;
            if (comparator != null) {
                map = f.create(comparator);
            } else {
                map = f.create();
            }
            if (!inMemory) {
                map = new MapDBWrapper(map);
                factories.put(new MapReference(map, queue), f);
            }
            return map;
        }
    }

    /**
     * A map wrapper implementation forwarding calls to a MapDB backed map
     * implementation. Instances of this class are tracked with weak references
     * and the corresponding {@link MapDBMapFactory} is disposed when there
     * are no more references to a MapDBWrapper. This also includes collections
     * obtained from this map, e.g. {@link #entrySet()}, {@link #values()} or
     * {@link #keySet()}.
     *
     */
    private static final class MapDBWrapper extends ForwardingConcurrentMap<String, Revision> {

        private final ConcurrentMap<String, Revision> map;

        protected MapDBWrapper(@Nonnull ConcurrentMap<String, Revision> map) {
            this.map = checkNotNull(map);
        }

        @Override
        protected ConcurrentMap<String, Revision> delegate() {
            return map;
        }

        @Override
        public Set<Entry<String, Revision>> entrySet() {
            return Collections.unmodifiableSet(map.entrySet());
        }

        @Override
        public Collection<Revision> values() {
            return Collections.unmodifiableCollection(map.values());
        }

        @Override
        public Set<String> keySet() {
            return Collections.unmodifiableSet(map.keySet());
        }
    }

    private static final class MapReference extends WeakReference<Object> {

        public MapReference(@Nonnull Object referent,
                            @Nonnull ReferenceQueue<Object> queue) {
            super(checkNotNull(referent), checkNotNull(queue));
        }
    }
}
