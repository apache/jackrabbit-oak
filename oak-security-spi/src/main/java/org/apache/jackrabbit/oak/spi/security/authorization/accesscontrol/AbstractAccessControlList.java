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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract base implementation of the {@code JackrabbitAccessControlList}
 * interface.
 */
public abstract class AbstractAccessControlList implements JackrabbitAccessControlList {

    private final String oakPath;
    private final NamePathMapper namePathMapper;

    public AbstractAccessControlList(@Nullable String oakPath,
                                     @NotNull NamePathMapper namePathMapper) {
        this.oakPath = oakPath;
        this.namePathMapper = namePathMapper;
    }

    //------------------------------------------< AbstractAccessControlList >---
    @Nullable
    public String getOakPath() {
        return oakPath;
    }

    @NotNull
    public NamePathMapper getNamePathMapper() {
        return namePathMapper;
    }

    @NotNull
    public abstract List<? extends JackrabbitAccessControlEntry> getEntries();

    @NotNull
    public abstract RestrictionProvider getRestrictionProvider();

    //--------------------------------------< JackrabbitAccessControlPolicy >---
    @Nullable
    @Override
    public String getPath() {
        return (oakPath == null) ? null : namePathMapper.getJcrPath(oakPath);
    }

    //--------------------------------------------------< AccessControlList >---

    @Override
    public AccessControlEntry[] getAccessControlEntries() {
        List<? extends JackrabbitAccessControlEntry> entries = getEntries();
        return entries.toArray(new JackrabbitAccessControlEntry[0]);
    }

    @Override
    public boolean addAccessControlEntry(Principal principal, Privilege[] privileges) throws RepositoryException {
        return addEntry(principal, privileges, true, Collections.<String, Value>emptyMap());
    }

    //----------------------------------------< JackrabbitAccessControlList >---

    @Override
    public boolean isEmpty() {
        return getEntries().isEmpty();
    }

    @Override
    public int size() {
        return getEntries().size();
    }

    @NotNull
    @Override
    public String[] getRestrictionNames() {
        return getRestrictionProvider().getSupportedRestrictions(getOakPath()).
            stream().map(definition -> namePathMapper.getJcrName(definition.getName())).
            toArray(String[]::new);
    }

    @Override
    public int getRestrictionType(@NotNull String restrictionName) {
        for (RestrictionDefinition definition : getRestrictionProvider().getSupportedRestrictions(getOakPath())) {
            String jcrName = namePathMapper.getJcrName(definition.getName());
            if (jcrName.equals(restrictionName)) {
                return definition.getRequiredType().tag();
            }
        }
        // for backwards compatibility with JR2 return undefined type for an
        // unknown restriction name.
        return PropertyType.UNDEFINED;
    }

    @Override
    public boolean isMultiValueRestriction(@NotNull String restrictionName) {
        for (RestrictionDefinition definition : getRestrictionProvider().getSupportedRestrictions(getOakPath())) {
            String jcrName = namePathMapper.getJcrName(definition.getName());
            if (jcrName.equals(restrictionName)) {
                return definition.getRequiredType().isArray();
            }
        }
        // not a supported restriction => return false.
        return false;
    }
    
    @Override
    public boolean addEntry(@NotNull Principal principal, @NotNull Privilege[] privileges, boolean isAllow) throws RepositoryException {
        return addEntry(principal, privileges, isAllow, Collections.<String, Value>emptyMap());
    }
    
    @Override
    public boolean addEntry(@NotNull Principal principal, @NotNull Privilege[] privileges, boolean isAllow, @Nullable Map<String, Value> restrictions) throws RepositoryException {
        return addEntry(principal, privileges, isAllow, restrictions, null);
    }
}
