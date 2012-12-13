/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mk.model;

import org.apache.jackrabbit.mk.store.Binding;
import org.apache.jackrabbit.mk.store.CacheObject;

import java.util.Iterator;

/**
 *
 */
public interface ChildNodeEntries extends Cloneable, CacheObject {
    
    static final int CAPACITY_THRESHOLD = 1000;

    Object clone();

    boolean inlined();

    //-------------------------------------------------------------< read ops >

    int getCount();

    ChildNodeEntry get(String name);

    Iterator<String> getNames(int offset, int count);

    Iterator<ChildNodeEntry> getEntries(int offset, int count);

    //------------------------------------------------------------< write ops >

    ChildNodeEntry add(ChildNodeEntry entry);

    ChildNodeEntry remove(String name);

    ChildNodeEntry rename(String oldName, String newName);

    //-------------------------------------------------------------< diff ops >

    /**
     * Returns those entries that exist in {@code other} but not in
     * {@code this}.
     *
     * @param other
     * @return
     */
    Iterator<ChildNodeEntry> getAdded(final ChildNodeEntries other);

    /**
     * Returns those entries that exist in {@code this} but not in
     * {@code other}.
     *
     * @param other
     * @return
     */
    Iterator<ChildNodeEntry> getRemoved(final ChildNodeEntries other);

    /**
     * Returns {@code this} instance's entries that have namesakes in
     * {@code other} but with different {@code id}s.
     *
     * @param other
     * @return
     */
    Iterator<ChildNodeEntry> getModified(final ChildNodeEntries other);

    //------------------------------------------------< serialization support >

    void serialize(Binding binding) throws Exception;
}
