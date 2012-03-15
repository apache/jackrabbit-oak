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

package org.apache.jackrabbit.oak.jcr.state;

import org.apache.jackrabbit.oak.jcr.util.Iterators;
import org.apache.jackrabbit.oak.model.AbstractNodeState;
import org.apache.jackrabbit.oak.model.ChildNodeEntry;
import org.apache.jackrabbit.oak.model.PropertyState;

import java.util.Iterator;

/**
 * A {@code NodeState} implementation which is empty. That is, does not
 * have properties nor child nodes.
 */
public final class EmptyNodeState extends AbstractNodeState {
    public static final EmptyNodeState INSTANCE = new EmptyNodeState();
    
    private EmptyNodeState() { }

    @Override
    public Iterable<PropertyState> getProperties() {
        return new Iterable<PropertyState>() {
            @Override
            public Iterator<PropertyState> iterator() {
                return Iterators.empty();
            }
        };
    }

    @Override
    public Iterable<ChildNodeEntry> getChildNodeEntries(long offset, int count) {
        return new Iterable<ChildNodeEntry>() {
            @Override
            public Iterator<ChildNodeEntry> iterator() {
                return Iterators.empty();
            }
        };
    }
}
