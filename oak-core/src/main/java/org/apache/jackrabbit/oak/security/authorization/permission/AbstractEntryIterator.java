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
package org.apache.jackrabbit.oak.security.authorization.permission;

import java.util.Iterator;
import java.util.NoSuchElementException;
import javax.annotation.CheckForNull;

import com.google.common.collect.Iterators;

/**
 * EntryIterator... TODO
 */
abstract class AbstractEntryIterator implements Iterator<PermissionEntry> {

    // the ordered permission entries at a given path in the hierarchy
    protected Iterator<PermissionEntry> nextEntries;

    // the next permission entry
    protected PermissionEntry next;

    @Override
    public boolean hasNext() {
        if (next == null) {
            // lazy initialization
            if (nextEntries == null) {
                nextEntries = Iterators.emptyIterator();
                seekNext();
            }
        }
        return next != null;
    }

    @Override
    public PermissionEntry next() {
        if (next == null) {
            throw new NoSuchElementException();
        }

        PermissionEntry pe = next;
        seekNext();
        return pe;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @CheckForNull
    protected abstract void seekNext();
}
