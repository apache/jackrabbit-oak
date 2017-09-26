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

package org.apache.jackrabbit.oak.plugins.document.bundlor;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.AbstractPropertyState;

//TODO Move this to org.apache.jackrabbit.oak.plugins.memory
class PropertyStateWrapper extends AbstractPropertyState implements PropertyState {
    private final PropertyState delegate;

    public PropertyStateWrapper(PropertyState delegate) {
        this.delegate = delegate;
    }

    @Nonnull
    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public boolean isArray() {
        return delegate.isArray();
    }

    @Override
    public Type<?> getType() {
        return delegate.getType();
    }

    @Nonnull
    @Override
    public <T> T getValue(Type<T> type) {
        return delegate.getValue(type);
    }

    @Nonnull
    @Override
    public <T> T getValue(Type<T> type, int index) {
        return delegate.getValue(type, index);
    }

    @Override
    public long size() {
        return delegate.size();
    }

    @Override
    public long size(int index) {
        return delegate.size(index);
    }

    @Override
    public int count() {
        return delegate.count();
    }
}
