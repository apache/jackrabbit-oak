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

/**
 * An iterator that pre-fetches a number of items in order to calculate the size
 * of the result if possible. This iterator loads at least a number of items,
 * and then tries to load some more items until the timeout is reached or the
 * maximum number of entries are read.
 * <p>
 * Prefeching is only done when size() is called.
 * 
 * @param <K> the iterator data type
 */
public class PrefetchIterator<K> implements Iterator<K> {

    private final Iterator<K> it;
    private final long minPrefetch, timeout, maxPrefetch;
    private boolean prefetchDone;
    private Iterator<K> prefetchIterator;
    private long size, position;
    
    /**
     * Create a new iterator.
     * 
     * @param it the base iterator
     * @param min the minimum number of items to pre-fetch
     * @param timeout the maximum time to pre-fetch in milliseconds
     * @param max the maximum number of items to pre-fetch
     * @param size the size (prefetching is only required if -1)
     */
    PrefetchIterator(Iterator<K> it, long min, long timeout, long max, long size) {
        this.it = it;
        this.minPrefetch = min;
        this.timeout = timeout;
        this.maxPrefetch = max;
        this.size = size;
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
        if (size != -1 || prefetchDone || position > maxPrefetch) {
            return size;
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
        while (position <= maxPrefetch) {
            if (position > minPrefetch) {
                if (end == 0 || System.nanoTime() > end) {
                    break;
                }
            }
            if (!it.hasNext()) {
                size = position;
                break;
            }
            position++;
            list.add(it.next());
        }
        if (list.size() > 0) {
            prefetchIterator = list.iterator();
            position -= list.size();
        }
        return size;
    }

}
