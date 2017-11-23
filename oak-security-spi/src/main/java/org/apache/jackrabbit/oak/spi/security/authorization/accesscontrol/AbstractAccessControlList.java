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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.Privilege;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;

/**
 * Abstract base implementation of the {@code JackrabbitAccessControlList}
 * interface.
 */
public abstract class AbstractAccessControlList implements JackrabbitAccessControlList {

    private final String oakPath;
    private final NamePathMapper namePathMapper;

    public AbstractAccessControlList(@Nullable String oakPath,
                                     @Nonnull NamePathMapper namePathMapper) {
        this.oakPath = oakPath;
        this.namePathMapper = namePathMapper;
    }

    //------------------------------------------< AbstractAccessControlList >---
    @CheckForNull
    public String getOakPath() {
        return oakPath;
    }

    @Nonnull
    public NamePathMapper getNamePathMapper() {
        return namePathMapper;
    }

    @Nonnull
    public abstract List<? extends JackrabbitAccessControlEntry> getEntries();

    @Nonnull
    public abstract RestrictionProvider getRestrictionProvider();

    //--------------------------------------< JackrabbitAccessControlPolicy >---
    @CheckForNull
    @Override
    public String getPath() {
        return (oakPath == null) ? null : namePathMapper.getJcrPath(oakPath);
    }

    //--------------------------------------------------< AccessControlList >---

    @Override
    public AccessControlEntry[] getAccessControlEntries() throws RepositoryException {
        List<? extends JackrabbitAccessControlEntry> entries = getEntries();
        return entries.toArray(new JackrabbitAccessControlEntry[entries.size()]);
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

    @Nonnull
    @Override
    public String[] getRestrictionNames() throws RepositoryException {
        Collection<RestrictionDefinition> supported = getRestrictionProvider().getSupportedRestrictions(getOakPath());
        return Collections2.transform(supported, new Function<RestrictionDefinition, String>() {
            @Override
            public String apply(RestrictionDefinition definition) {
                return namePathMapper.getJcrName(definition.getName());
            }
        }).toArray(new String[supported.size()]);

    }

    @Override
    public int getRestrictionType(String restrictionName) throws RepositoryException {
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
    public boolean isMultiValueRestriction(String restrictionName) throws RepositoryException {
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
    public boolean addEntry(Principal principal, Privilege[] privileges, boolean isAllow) throws RepositoryException {
        return addEntry(principal, privileges, isAllow, Collections.<String, Value>emptyMap());
    }
    @Override
    public boolean addEntry(Principal principal, Privilege[] privileges, boolean isAllow, Map<String, Value> restrictions) throws RepositoryException {
        return addEntry(principal, privileges, isAllow, restrictions, null);
    }
}