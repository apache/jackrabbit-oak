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
public abstract class AbstractFilteringIterator<T> implements Iterator<T> {

    protected final Iterator<T> it;
    protected int count;

    protected T next = null;

    public AbstractFilteringIterator(Iterator<T> it) {
        this.it = it;
    }

    public boolean hasNext() {
        while (next == null && it.hasNext()) {
            T entry = it.next();
            if (include(entry)) {
                next = entry;
            }
        }

        return next != null;
    }
    
    abstract protected boolean include(T entry);

    public T next() {
        if (hasNext()) {
            T entry = next;
            next = null;
            return entry;
        } else {
            throw new NoSuchElementException();
        }
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
}
