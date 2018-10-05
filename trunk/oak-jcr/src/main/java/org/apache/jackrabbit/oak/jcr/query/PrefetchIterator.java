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
package org.apache.jackrabbit.oak.jcr.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Result.SizePrecision;

/**
 * An iterator that pre-fetches a number of items in order to calculate the size
 * of the result if possible. This iterator loads at least a number of items,
 * and then tries to load some more items until the timeout is reached or the
 * maximum number of entries are read.
 * <p>
 * Prefetching is only done when size() is called.
 * 
 * @param <K> the iterator data type
 */
public class PrefetchIterator<K> implements Iterator<K> {
    
    private final Iterator<K> it;
    private final long minPrefetch, timeout, maxPrefetch;
    private final boolean fastSize;
    private final Result fastSizeCallback;
    private boolean prefetchDone;
    private Iterator<K> prefetchIterator;
    private long size, position;

    /**
     * Create a new iterator.
     * 
     * @param it the base iterator
     * @param options the prefetch options to use
     */
    PrefetchIterator(Iterator<K> it, PrefetchOptions options) {
        this.it = it;
        this.minPrefetch = options.min;
        this.maxPrefetch = options.max;
        this.timeout = options.fastSize ? 0 : options.timeout;
        this.fastSize = options.fastSize;
        this.size = options.size;
        this.fastSizeCallback = options.fastSizeCallback;
    }

    @Override
    public boolean hasNext() {
        if (prefetchIterator != null) {
            if (prefetchIterator.hasNext()) {
                return true;
            }
            prefetchIterator = null;
        }
        boolean result = it.hasNext();
        if (!result) {
            size = position;
        }
        return result;
    }

    @Override
    public K next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        if (prefetchIterator != null) {
            return prefetchIterator.next();
        }
        position++;
        return it.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    /**
     * Get the size if known. This call might pre-fetch data. The returned value
     * is unknown if the actual size is larger than the number of pre-fetched
     * elements, unless the end of the iterator has been reached already.
     * 
     * @return the size, or -1 if unknown
     */
    public long size() {
        if (size != -1) {
            return size;
        }
        if (!fastSize) {
            if (prefetchDone || position > maxPrefetch) {
                return -1;
            }
        }
        prefetchDone = true;
        ArrayList<K> list = new ArrayList<K>();
        long end;
        if (timeout <= 0) {
            end = 0;
        } else {
            long nanos = System.nanoTime();
            end = nanos + timeout * 1000 * 1000;
        }
        while (true) {
            if (!it.hasNext()) {
                size = position;
                break;
            }
            if (position > maxPrefetch) {
                break;
            }
            if (position > minPrefetch) {
                if (end == 0 || System.nanoTime() > end) {
                    break;
                }
            }
            position++;
            list.add(it.next());
        }
        if (list.size() > 0) {
            prefetchIterator = list.iterator();
            position -= list.size();
        }
        if (size == -1 && fastSize) {
            if (fastSizeCallback != null) {
                size = fastSizeCallback.getSize(SizePrecision.EXACT, Long.MAX_VALUE);
            }
        }
        return size;
    }
    
    /**
     * The options to use for prefetching.
     */
    public static class PrefetchOptions {
        
        // uses the "simple" named-parameter pattern
        // see also http://stackoverflow.com/questions/1988016/named-parameter-idiom-in-java
        
        /**
         * The minimum number of rows / nodes to pre-fetch.
         */
        long min = 20;
        
        /**
         * The maximum number of rows / nodes to pre-fetch.
         */
        long max = 100;
        
        /**
         * The maximum number of milliseconds to prefetch rows / nodes
         * (ignored if fastSize is set).
         */
        long timeout = 100;
        
        /**
         * The size if known, or -1 if not (prefetching is only required if -1).
         */
        long size;
        
        /**
         * Whether or not the expected size should be read from the result.
         */
        boolean fastSize;
        
        /**
         * The result (optional) to get the size from, in case the fast size options is set.
         */
        Result fastSizeCallback;
        
        {
            String s = System.getProperty("oak.queryMinPrefetch");
            if (s != null) {
                try {
                    min = Integer.parseInt(s);
                    max = Math.max(min, max);
                } catch (Exception e) {
                    // ignore
                }
            }
        }
        
    }

}
