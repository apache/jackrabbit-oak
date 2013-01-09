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
package org.apache.jackrabbit.oak.spi.security.authorization;

import java.security.Principal;
import java.util.Collections;
import java.util.Set;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.Privilege;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;

/**
 * AbstractAccessControlList... TODO
 */
public abstract class AbstractAccessControlList implements JackrabbitAccessControlList {

    protected final String jcrPath;
    protected final RestrictionProvider restrictionProvider;

    public AbstractAccessControlList(String jcrPath, RestrictionProvider restrictionProvider) {
        this.jcrPath = jcrPath;
        this.restrictionProvider = restrictionProvider;
    }

    //--------------------------------------< JackrabbitAccessControlPolicy >---
    @Override
    public String getPath() {
        return jcrPath;
    }

    //--------------------------------------------------< AccessControlList >---

    @Override
    public boolean addAccessControlEntry(Principal principal, Privilege[] privileges) throws RepositoryException {
        return addEntry(principal, privileges, true, Collections.<String, Value>emptyMap());
    }

    //----------------------------------------< JackrabbitAccessControlList >---
    @Override
    public String[] getRestrictionNames() throws RepositoryException {
        Set<RestrictionDefinition> supported = restrictionProvider.getSupportedRestrictions(jcrPath);
        return Collections2.transform(supported, new Function<RestrictionDefinition, String>() {
            @Override
            public String apply(RestrictionDefinition definition) {
                return definition.getJcrName();
            }
        }).toArray(new String[supported.size()]);

    }

    @Override
    public int getRestrictionType(String restrictionName) throws RepositoryException {
        for (RestrictionDefinition definition : restrictionProvider.getSupportedRestrictions(jcrPath)) {
            if (definition.getJcrName().equals(restrictionName)) {
                return definition.getRequiredType();
            }
        }
        return PropertyType.UNDEFINED;
    }

    @Override
    public boolean addEntry(Principal principal, Privilege[] privileges, boolean isAllow) throws RepositoryException {
        return addEntry(principal, privileges, isAllow, Collections.<String, Value>emptyMap());
    }
}