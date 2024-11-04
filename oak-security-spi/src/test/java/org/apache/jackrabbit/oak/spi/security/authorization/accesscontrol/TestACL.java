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
package org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol;

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.jcr.Value;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Test implementation of AbstractAccessControlList
 */
public class TestACL extends AbstractAccessControlList {

    private final List<JackrabbitAccessControlEntry> entries = new ArrayList<>();
    private final RestrictionProvider restrictionProvider;

    public TestACL(@Nullable String jcrPath,
                   @NotNull RestrictionProvider restrictionProvider,
                   @NotNull NamePathMapper namePathMapper,
                   @NotNull List<JackrabbitAccessControlEntry> entries) {
        super((jcrPath == null) ? null : namePathMapper.getOakPath(jcrPath), namePathMapper);
        this.entries.addAll(entries);
        this.restrictionProvider = restrictionProvider;
    }

    public TestACL(@Nullable String jcrPath,
                   @NotNull RestrictionProvider restrictionProvider,
                   @NotNull NamePathMapper namePathMapper,
                   @NotNull JackrabbitAccessControlEntry... entry) {
        this(jcrPath, restrictionProvider, namePathMapper, List.of(entry));
    }

    @Override
    public boolean isEmpty() {
        return entries.isEmpty();
    }

    @Override
    public int size() {
        return entries.size();
    }

    @Override
    public boolean addEntry(@NotNull Principal principal, @NotNull Privilege[] privileges, boolean isAllow, @Nullable Map<String, Value> restrictions, @Nullable Map<String, Value[]> mvRestrictions) {
        return false;
    }

    @Override
    public void orderBefore(@NotNull AccessControlEntry srcEntry, @Nullable AccessControlEntry destEntry) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeAccessControlEntry(AccessControlEntry ace) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public List<JackrabbitAccessControlEntry> getEntries() {
        return entries;
    }

    @NotNull
    @Override
    public RestrictionProvider getRestrictionProvider() {
        return restrictionProvider;
    }
}
