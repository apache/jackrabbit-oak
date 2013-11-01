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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.util.TreeUtil;

/**
 * <code>PermissionEntries</code> holds the permission entries of one principal
 */
public class PrincipalPermissionEntries {

    /**
     * principal name
     */
    private final String name;

    /**
     * timestamp of the per-principal permission store
     */
    private long timestamp;

    /**
     * map of permission entries, accessed by path
     */
    private Map<String, Collection<PermissionEntry>> entries = new HashMap<String, Collection<PermissionEntry>>();

    public PrincipalPermissionEntries(@Nonnull String name) {
        this.name = name;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Nonnull
    public Collection<PermissionEntry> getEntries(@Nonnull String path) {
        Collection<PermissionEntry> ret = entries.get(path);
        return ret == null ? Collections.<PermissionEntry>emptyList() : ret;
    }

    @Nonnull
    public Map<String, Collection<PermissionEntry>> getEntries() {
        return entries;
    }

}