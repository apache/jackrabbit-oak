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
package org.apache.jackrabbit.oak.spi.state;

import org.apache.jackrabbit.oak.api.PropertyState;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Proxy node state that delegates all method calls to another instance.
 * Mostly useful as a base class for more complicated decorator functionality.
 */
public class ProxyNodeState implements NodeState {

    protected NodeState delegate;

    protected ProxyNodeState(NodeState delegate) {
        assert delegate != null;
        this.delegate = delegate;
    }

    @Override
    public PropertyState getProperty(String name) {
        return delegate.getProperty(name);
    }

    @Override
    public long getPropertyCount() {
        return delegate.getPropertyCount();
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return delegate.getProperties();
    }

    @Override
    public NodeState getChildNode(String name) {
        return delegate.getChildNode(name);
    }

    @Override
    public long getChildNodeCount() {
        return delegate.getChildNodeCount();
    }

    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return delegate.getChildNodeEntries();
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("{");
        AtomicBoolean first = new AtomicBoolean(true);
        for (PropertyState property : getProperties()) {
            if (!first.getAndSet(false)) {
                builder.append(',');
            }
            builder.append(' ').append(property);
        }
        for (ChildNodeEntry entry : getChildNodeEntries()) {
            if (!first.getAndSet(false)) {
                builder.append(',');
            }
            builder.append(' ').append(entry);
        }
        builder.append(" }");
        return builder.toString();
    }

    @Override
    public boolean equals(Object that) {
        // Note the careful ordering of calls here. We need to undo the
        // proxying, but still allow a possible subclass to implement
        // custom equality checks if it wants to. Thus we don't simply
        // look inside another ProxyNodeState instance, but rather let
        // it handle the equality check against our delegate instance.
        // If it's just another call to this same method, the resulting
        // final equality check will be that.delegate.equals(delegate).
        if (that instanceof ProxyNodeState) {
            return that.equals(delegate);
        } else {
            return delegate.equals(that);
        }
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

}
