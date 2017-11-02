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

package org.apache.jackrabbit.oak.blob.composite;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.spi.blob.DataStoreProvider;

import java.util.Map;

public class CompositeDataStoreDelegate {
    private final DataStoreProvider ds;
    private final Map<String, Object> config;

    CompositeDataStoreDelegate(final DataStoreProvider ds, final String role, final Map<String, Object> config) {
        this.ds = ds;
        if (null != config) {
            this.config = config;
        }
        else {
            this.config = Maps.newConcurrentMap();
        }
        this.config.put(DataStoreProvider.ROLE, role);
    }

    public DataStoreProvider getDataStore() {
        return ds;
    }

    public String getRole() {
        return (String) config.get(DataStoreProvider.ROLE);
    }

    public Map<String, ?> getConfig() {
        return config;
    }

    public static CompositeDataStoreDelegateBuilder builder(final DataStoreProvider ds) {
        return new CompositeDataStoreDelegateBuilder(ds);
    }

    static class CompositeDataStoreDelegateBuilder {
        DataStoreProvider ds = null;
        String role = null;
        Map<String, Object> config = Maps.newConcurrentMap();

        public CompositeDataStoreDelegateBuilder(final DataStoreProvider ds) {
            this.ds = ds;
        }

        public CompositeDataStoreDelegateBuilder withRole(final String role) {
            this.role = role;
            return this;
        }

        public CompositeDataStoreDelegateBuilder withConfig(final Map<String, Object> config) {
            this.config = config;
            return this;
        }

        public CompositeDataStoreDelegate build() {
            if (Strings.isNullOrEmpty(role) && null != config) {
                role = (String) config.get(DataStoreProvider.ROLE);
            }
            if (null != ds && ! Strings.isNullOrEmpty(role)) {
                return new CompositeDataStoreDelegate(ds, role, config);
            }
            return null;
        }
    }
}