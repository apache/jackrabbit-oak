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
package org.apache.jackrabbit.oak.security.authorization.accesscontrol;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.util.Map;

import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_GLOB;

final class TestUtility {
    
    private TestUtility() {}

    @NotNull
    static ACL getApplicablePolicy( @NotNull AccessControlManager acMgr, @Nullable String path) throws RepositoryException {
        AccessControlPolicyIterator itr = acMgr.getApplicablePolicies(path);
        if (itr.hasNext()) {
            return (ACL) itr.nextAccessControlPolicy();
        } else {
            throw new RepositoryException("No applicable policy found.");
        }
    }

    @NotNull
    static ACL setupPolicy(@NotNull AccessControlManager acMgr,
                           @Nullable String path,
                           @NotNull Principal testPrincipal,
                           @NotNull Privilege[] privileges,
                           boolean isAllow,
                           @Nullable Map<String, Value> restrictions,
                           @Nullable Map<String, Value[]> mvRestrictions) throws RepositoryException {
        ACL policy = getApplicablePolicy(acMgr, path);
        if (path == null) {
            policy.addAccessControlEntry(testPrincipal, privileges);
        } else {
            policy.addEntry(testPrincipal, privileges, isAllow, restrictions, mvRestrictions);
        }
        acMgr.setPolicy(path, policy);
        return policy;
    }

    @NotNull
    static Map<String, Value> getGlobRestriction(@NotNull String value, @NotNull ValueFactory valueFactory) {
        return Map.of(REP_GLOB, valueFactory.createValue(value));
    }
}