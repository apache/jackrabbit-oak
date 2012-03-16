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
package org.apache.jackrabbit.mk.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 *
 */
public abstract class AbstractRangeIterator<T> implements Iterator<T> {

    protected final Iterator<?> it;
    protected int maxCount;

    public AbstractRangeIterator(Iterator<?> it, int offset, int count) {
        while (offset-- > 0 && it.hasNext()) {
            it.next();
        }
        maxCount = count < 0 ? Integer.MAX_VALUE : count;
        this.it = it;
    }

    public boolean hasNext() {
        return (maxCount > 0 && it.hasNext());
    }

    public T next() {
        if (maxCount-- > 0) {
            return doNext();
        }
        throw new NoSuchElementException();
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    protected abstract T doNext();
}
