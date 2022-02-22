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
package org.apache.jackrabbit.oak.benchmark.authorization;

import joptsimple.internal.Strings;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.security.Privilege;
import java.security.Principal;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_GLOB;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_GLOBS;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_SUBTREES;

public class MvGlobsAndSubtreesTest extends HasPermissionHasItemGetItemTest {

    private enum RestrictionType {
        REP_GLOB,
        REP_GLOBS,
        REP_SUBTREES
    }
    
    private static final String GLOB1 = "*/jcr:content";
    private static final String GLOB2 = "*/jcr:content/*";
    private static final String SUBTREE = "/jcr:content";
    
    private final RestrictionType restrictionType;

    public MvGlobsAndSubtreesTest(int itemsToRead, int numberOfACEs, int numberOfGroups, boolean doReport, String restrictionType) {
        super(itemsToRead, numberOfACEs, numberOfGroups, doReport);

        this.restrictionType = getRestrictionType(restrictionType);
    }

    @NotNull
    private static RestrictionType getRestrictionType(@Nullable String type) {
        if (Strings.isNullOrEmpty(type)) {
            return RestrictionType.REP_SUBTREES;
        }
        try {
            return RestrictionType.valueOf(type);
        } catch (IllegalArgumentException e) {
            return RestrictionType.REP_SUBTREES;
        }
    }

    @Override
    @NotNull String additionalMethodName() {
        return super.additionalMethodName() + " with ac setup including restriction '"+restrictionType+"')";
    }

    @Override
    boolean createEntry(@NotNull JackrabbitAccessControlManager acMgr, @NotNull Principal principal, @NotNull String path,
                        @NotNull Privilege[] privileges) throws RepositoryException {
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, path);
        if (acl == null) {
            throw new IllegalStateException("No policy to setup ACE.");
        }
        
        ValueFactory vf = adminSession.getValueFactory();
        
        boolean added;
        if (restrictionType == RestrictionType.REP_GLOB) {
            added = (acl.addEntry(principal, privileges, true, singletonMap(REP_GLOB, vf.createValue(GLOB1)), emptyMap()) ||
                     acl.addEntry(principal, privileges, true, singletonMap(REP_GLOB, vf.createValue(GLOB2)), emptyMap()));
        } else if (restrictionType == RestrictionType.REP_GLOBS) {
            added = acl.addEntry(principal, privileges, true, emptyMap(), 
                    singletonMap(REP_GLOBS, new Value[] {vf.createValue(GLOB1), vf.createValue(GLOB2)}));
        } else {
            // rep:subtrees
            added = acl.addEntry(principal, privileges, true, emptyMap(),
                    singletonMap(REP_SUBTREES, new Value[] {vf.createValue(SUBTREE)}));
        }
        if (added) {
            acMgr.setPolicy(acl.getPath(), acl);
        }
        return added;
    }

}