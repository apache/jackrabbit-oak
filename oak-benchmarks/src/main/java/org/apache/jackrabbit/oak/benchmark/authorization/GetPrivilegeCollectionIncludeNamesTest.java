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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import joptsimple.internal.Strings;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.authorization.PrivilegeCollection;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class GetPrivilegeCollectionIncludeNamesTest extends AbstractHasItemGetItemTest {
    
    private enum EvaluationType {
        ACCESSCONTORL_MANAGER_GET_PRIVILEGE_COLLECTION,
        JCR_PRIVILEGE_NAME_AGGREGATION,
        ACCESSCONTORL_MANAGER_HAS_PRIVILEGES
    } 
    
    private static final List<String> ALL_PRIVILEGE_NAMES = ImmutableList.copyOf(PrivilegeBits.BUILT_IN.keySet());
    
    private final EvaluationType evalType;
    
    public GetPrivilegeCollectionIncludeNamesTest(int itemsToRead, int numberOfACEs, int numberOfGroups, boolean doReport, String evalType) {
        super(itemsToRead, numberOfACEs, numberOfGroups, doReport);
        this.evalType = getEvalType(evalType);
    }
    
    @NotNull
    private static EvaluationType getEvalType(@Nullable String type) {
        if (Strings.isNullOrEmpty(type)) {
            return EvaluationType.ACCESSCONTORL_MANAGER_GET_PRIVILEGE_COLLECTION;
        }
        try {
            return EvaluationType.valueOf(type);
        } catch (IllegalArgumentException e) {
            return EvaluationType.ACCESSCONTORL_MANAGER_GET_PRIVILEGE_COLLECTION;
        }
    }

    @Override
    @NotNull String additionalMethodName() {
        return evalType.name();
    }

    @Override
    void additionalOperations(@NotNull String path, @NotNull Session s, @NotNull AccessControlManager acMgr) {
        try {
            List<String> privNames = ImmutableList.of(getRandom(ALL_PRIVILEGE_NAMES), getRandom(ALL_PRIVILEGE_NAMES), getRandom(ALL_PRIVILEGE_NAMES), getRandom(ALL_PRIVILEGE_NAMES));
            String accessControlledPath = getAccessControlledPath(path);
            if (EvaluationType.ACCESSCONTORL_MANAGER_GET_PRIVILEGE_COLLECTION == evalType) {
                PrivilegeCollection pc = ((JackrabbitAccessControlManager) acMgr).getPrivilegeCollection(accessControlledPath);
                for (String toTest : privNames) {
                    boolean includes = pc.includes(toTest);
                    if (doReport) {
                        System.out.println("PrivilegeCollection.includes('"+toTest+"') : "+includes);
                    }
                }
            } else if (EvaluationType.JCR_PRIVILEGE_NAME_AGGREGATION == evalType) {
                // evaluation using regular JCR Privilege API 
                Privilege[] privileges = acMgr.getPrivileges(accessControlledPath);
                Set<String> privilegeNames = Sets.newHashSet(AccessControlUtils.namesFromPrivileges(privileges));
                Stream.of(privileges).filter(Privilege::isAggregate).forEach(privilege -> Collections.addAll(privilegeNames, AccessControlUtils.namesFromPrivileges(privilege.getAggregatePrivileges())));
                for (String toTest : privNames) {
                    boolean includes = privilegeNames.contains(toTest);
                    if (doReport) {
                        System.out.println("Privileges '"+Arrays.toString(privileges)+"' include '"+toTest+"' : "+includes);
                    }
                }
            } else {
                // evaluation using separate AccessControlManager.hasPrivileges
                for (String toTest : privNames) {
                    boolean hasPrivilege = acMgr.hasPrivileges(accessControlledPath, AccessControlUtils.privilegesFromNames(acMgr, toTest)); 
                    if (doReport) {
                        System.out.println("AccessControlManager.hasPrivileges('"+accessControlledPath+"','"+toTest+"') : "+hasPrivilege);
                    }
                }
            }
        } catch (RepositoryException e) {
            if (doReport) {
                e.printStackTrace(System.out);
            }
        }
    }

    @Override
    protected void randomRead(Session testSession, List<String> allPaths, int cnt) throws RepositoryException {
        boolean logout = false;
        if (testSession == null) {
            testSession = getTestSession();
            logout = true;
        }
        try {
            int nodeCnt = 0;
            int propertyCnt = 0;
            int addCnt = 0;
            int noAccess = 0;
            int size = allPaths.size();

            AccessControlManager acMgr = testSession.getAccessControlManager();

            long start = System.currentTimeMillis();
            for (int i = 0; i < cnt; i++) {
                double rand = size * Math.random();
                int index = (int) Math.floor(rand);
                String path = allPaths.get(index);
                additionalOperations(path, testSession, acMgr);
            }
            long end = System.currentTimeMillis();
            if (doReport) {
                System.out.println("Session " + testSession.getUserID() + " reading " + cnt + " (Nodes: "+ nodeCnt +"; Properties: "+propertyCnt+"; no access: "+noAccess+"; "+ additionalMethodName()+": "+addCnt+") completed in " + (end - start));
            }
        } finally {
            if (logout) {
                logout(testSession);
            }
        }
    }
}