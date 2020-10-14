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

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Utils {

    private static final Logger log = LoggerFactory.getLogger(Utils.class);


    @NotNull
    public static Collection getRandom(@NotNull List<?> objects, int len) {
        int size = objects.size();
        if (len > size) {
            throw new IllegalArgumentException();
        } else if (len == size) {
            return objects;
        } else {
            Set s = new HashSet(len);
            while (s.size() < len) {
                int index = (int) Math.floor(size * Math.random());
                s.add(objects.get(index));
            }
            return s;
        }
    }

    public static boolean addEntry(@NotNull JackrabbitAccessControlManager acMgr, @NotNull Principal principal, @NotNull String path, @NotNull Privilege[] privileges) throws RepositoryException {
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, path);
        if (acl == null) {
            throw new IllegalStateException("No policy to setup ACE.");
        }
        boolean added = acl.addAccessControlEntry(principal, privileges);
        if (added) {
            acMgr.setPolicy(acl.getPath(), acl);
        }
        return added;
    }

    public static void removePrincipals(@NotNull Set<Principal> principalSet, @NotNull Session session) throws RepositoryException {
        UserManager userManager = ((JackrabbitSession) session).getUserManager();
        for (Principal p : principalSet) {
            Authorizable a = userManager.getAuthorizable(p);
            if (a != null) {
                a.remove();
            }
        }
        session.save();
    }
}