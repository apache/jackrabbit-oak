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

package org.apache.jackrabbit.oak.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A {@code PagedIterator} is an iterator of several pages. A page itself is
 * an iterator. The abstract {@code getPage} method is called whenever this
 * iterator needs to fetch another page.<p/>
 *
 * Lazy flattening (e.g. with {@link Iterators#flatten(java.util.Iterator)}
 * results in an iterator which does batch reading from its back end.
 *
 * @param <T>
 */
public abstract class PagedIterator<T> implements Iterator<Iterator<? extends T>> {
    private final int pageSize;
    private long pos;
    private Iterator<? extends T> current;

    protected PagedIterator(int pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * @param pos  start index
     * @param size  maximal number of elements
     * @return  iterator starting at index {@code pos} containing at most {@code size} elements.
     */
    protected abstract Iterator<? extends T> getPage(long pos, int size);

    @Override
    public boolean hasNext() {
        if (current == null) {
            current = getPage(pos, pageSize);
            pos += pageSize;
        }

        return current.hasNext();
    }

    @Override
    public Iterator<? extends T> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Iterator<? extends T> e = current;
        current = null;
        return e;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }
}
