/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.jackrabbit.oak.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Filtering iterators that are useful for queries with limit, offset, order by,
 * or distinct.
 */
public class FilterIterators {
    
    public static <K> Iterator<K> newCombinedFilter(
            Iterator<K> it, boolean distinct, long limit, long offset, 
            Comparator<K> orderBy) {
        if (distinct) {
            it = FilterIterators.newDistinct(it);
        }
        if (orderBy != null) {
            // avoid overflow (both offset and limit could be Long.MAX_VALUE)
            int max = (int) Math.min(Integer.MAX_VALUE, 
                    Math.min(Integer.MAX_VALUE, offset) + 
                    Math.min(Integer.MAX_VALUE, limit));
            it = FilterIterators.newSort(it, orderBy, max);
        }
        if (offset != 0) {
            it = FilterIterators.newOffset(it, offset);
        }
        if (limit < Long.MAX_VALUE) {
            it = FilterIterators.newLimit(it, limit);
        }
        return it;
    }
    
    public static <K> DistinctIterator<K> newDistinct(Iterator<K> it) {
        return new DistinctIterator<K>(it);
    }
    
    public static <K> Iterator<K> newLimit(Iterator<K> it, long limit) {
        return new LimitIterator<K>(it, limit);
    }
    
    public static <K> Iterator<K> newOffset(Iterator<K> it, long offset) {
        return new OffsetIterator<K>(it, offset);
    }
    
    public static <K> Iterator<K> newSort(Iterator<K> it, Comparator<K> orderBy, int max) {
        return new SortIterator<K>(it, orderBy, max);
    }

    /**
     * An iterator that filters duplicate entries, that is, it only returns each
     * unique entry once. The internal set of unique entries is filled only when
     * needed (on demand).
     * 
     * @param <K> the entry type
     */
    static class DistinctIterator<K> implements Iterator<K> {

        private final Iterator<K> source;
        private final HashSet<K> distinctSet;
        private K current;
        private boolean end;

        DistinctIterator(Iterator<K> source) {
            this.source = source;
            distinctSet = new HashSet<K>();
        }

        private void fetchNext() {
            if (end) {
                return;
            }
            while (source.hasNext()) {
                current = source.next();
                if (distinctSet.add(current)) {
                    return;
                }
            }
            current = null;
            end = true;
        }

        @Override
        public boolean hasNext() {
            if (current == null) {
                fetchNext();
            }
            return !end;
        }

        @Override
        public K next() {
            if (end) {
                throw new NoSuchElementException();
            }
            if (current == null) {
                fetchNext();
            }
            K r = current;
            current = null;
            return r;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }
    
    /**
     * An iterator that returns entries in sorted order. The internal list of
     * sorted entries can be limited to a given number of entries, and the
     * entries are only read when needed (on demand).
     * 
     * @param <K> the entry type
     */
    static class SortIterator<K> implements Iterator<K> {

        private final Iterator<K> source;
        private final Comparator<K> orderBy;
        private Iterator<K> result;
        private final int max;

        SortIterator(Iterator<K> source, Comparator<K> orderBy, int max) {
            this.source = source;
            this.orderBy = orderBy;
            this.max = max;
        }
        
        private void init() {
            if (result != null) {
                return;
            }
            ArrayList<K> list = new ArrayList<K>();
            while (source.hasNext()) {
                K x = source.next();
                list.add(x);
                // from time to time, sort and truncate
                // this should results in O(n*log(2*keep)) operations,
                // which is close to the optimum O(n*log(keep))
                if (list.size() > max * 2) {
                    // remove tail entries right now, to save memory
                    Collections.sort(list, orderBy);
                    keepFirst(list, max);
                }
            }
            Collections.sort(list, orderBy);
            keepFirst(list, max);
            result = list.iterator();
        }
        
        /**
         * Truncate a list.
         * 
         * @param list the list
         * @param keep the maximum number of entries to keep
         */
        private static <K> void keepFirst(ArrayList<K> list, int keep) {
            while (list.size() > keep) {
                // remove the entries starting at the end, 
                // to avoid n^2 performance
                list.remove(list.size() - 1);
            }        
        }

        @Override
        public boolean hasNext() {
            init();
            return result.hasNext();
        }

        @Override
        public K next() {
            init();
            return result.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
        
    }
    
    /**
     * An iterator that ignores the first number of entries. Entries are only
     * read when needed (on demand).
     * 
     * @param <K> the entry type
     */
    static class OffsetIterator<K> implements Iterator<K> {

        private final Iterator<K> source;
        private final long offset;
        private boolean init;

        OffsetIterator(Iterator<K> source, long offset) {
            this.source = source;
            this.offset = offset;
        }
        
        private void init() {
            if (init) {
                return;
            }
            init = true;
            for (int i = 0; i < offset && source.hasNext(); i++) {
                source.next();
            }
        }

        @Override
        public boolean hasNext() {
            init();
            return source.hasNext();
        }

        @Override
        public K next() {
            init();
            return source.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
        
    }
    
    /**
     * An iterator that limits the number of returned entries. Entries are only
     * read when needed (on demand).
     * 
     * @param <K> the entry type
     */
    static class LimitIterator<K> implements Iterator<K> {

        private final Iterator<K> source;
        private final long limit;
        private long count;

        LimitIterator(Iterator<K> source, long limit) {
            this.source = source;
            this.limit = limit;
        }
        
        @Override
        public boolean hasNext() {
            return count < limit && source.hasNext();
        }

        @Override
        public K next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            count++;
            return source.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
        
    }

}
