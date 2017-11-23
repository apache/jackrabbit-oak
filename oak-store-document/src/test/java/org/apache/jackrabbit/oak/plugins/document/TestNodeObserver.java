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

package org.apache.jackrabbit.oak.plugins.document;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.observation.NodeObserver;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;

public class TestNodeObserver extends NodeObserver {
    public final Map<String, Set<String>> added = newHashMap();
    public final Map<String, Set<String>> deleted = newHashMap();
    public final Map<String, Set<String>> changed = newHashMap();
    public final Map<String, Map<String, String>> properties = newHashMap();

    public TestNodeObserver(String path, String... propertyNames) {
        super(path, propertyNames);
    }

    @Override
    protected void added(
            @Nonnull String path,
            @Nonnull Set<String> added,
            @Nonnull Set<String> deleted,
            @Nonnull Set<String> changed,
            @Nonnull Map<String, String> properties,
            @Nonnull CommitInfo commitInfo) {
        this.added.put(path, newHashSet(added));
        if (!properties.isEmpty()) {
            this.properties.put(path, newHashMap(properties));
        }
    }

    @Override
    protected void deleted(
            @Nonnull String path,
            @Nonnull Set<String> added,
            @Nonnull Set<String> deleted,
            @Nonnull Set<String> changed,
            @Nonnull Map<String, String> properties,
            @Nonnull CommitInfo commitInfo) {
        this.deleted.put(path, newHashSet(deleted));
        if (!properties.isEmpty()) {
            this.properties.put(path, newHashMap(properties));
        }
    }

    @Override
    protected void changed(
            @Nonnull String path,
            @Nonnull Set<String> added,
            @Nonnull Set<String> deleted,
            @Nonnull Set<String> changed,
            @Nonnull Map<String, String> properties,
            @Nonnull CommitInfo commitInfo) {
        this.changed.put(path, newHashSet(changed));
        if (!properties.isEmpty()) {
            this.properties.put(path, newHashMap(properties));
        }
    }

    public void reset(){
        added.clear();
        deleted.clear();
        changed.clear();
        properties.clear();
    }
}
