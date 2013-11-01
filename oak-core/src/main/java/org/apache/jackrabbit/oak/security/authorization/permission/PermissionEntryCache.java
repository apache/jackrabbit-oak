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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <code>PermissionEntryCache</code> caches the permission entries of principals. The cache is held globally and contains
 * a version of the principal permission entries of the session that read them last. each session gets a lazy copy of
 * the cache and needs to verify if each cached principal permission set still reflects the state that the session sees.
 * every newly loaded principal permission set can be pushed down to the base cache if it does not exist there yet, or
 * if it's newer.
 */
public class PermissionEntryCache {

    private final Map<String, PrincipalPermissionEntries> base = new ConcurrentHashMap<String, PrincipalPermissionEntries>();

    public Local createLocalCache() {
        return new Local();
    }

    public void flush(Set<String> principalNames) {
        base.keySet().removeAll(principalNames);
    }

    public class Local {

        private final Map<String, PrincipalPermissionEntries> entries = new HashMap<String, PrincipalPermissionEntries>();

        private final Set<String> verified = new HashSet<String>();

        public Local() {
            entries.putAll(base);
        }

        public PrincipalPermissionEntries getEntries(PermissionStore store, String principalName) {
            PrincipalPermissionEntries ppe = entries.get(principalName);
            if (ppe == null) {
                ppe = store.load(principalName);
                entries.put(principalName, ppe);
            } else {
                if (!verified.contains(principalName)) {
                    if (store.getTimestamp(principalName) != ppe.getTimestamp()) {
                        ppe = store.load(principalName);
                        entries.put(principalName, ppe);
                    }
                    verified.add(principalName);
                }
            }

            // check if base cache has the entries
            PrincipalPermissionEntries baseppe = base.get(principalName);
            if (baseppe == null || ppe.getTimestamp() > baseppe.getTimestamp()) {
                base.put(principalName, ppe);
            }
            return ppe;
        }

        public boolean hasEntries(PermissionStore store, String principalName) {
            return getNumEntries(store, principalName) > 0;
        }

        public long getNumEntries(PermissionStore store, String principalName) {
            return getEntries(store, principalName).getEntries().size();
        }

        public void flush(Set<String> principalNames) {
            verified.removeAll(principalNames);
        }

    }
}