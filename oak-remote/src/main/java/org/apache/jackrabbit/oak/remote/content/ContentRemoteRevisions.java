/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.remote.content;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.Root;

import java.security.Principal;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

class ContentRemoteRevisions {

    private class Key {

        private final String revisionId;

        private final Set<Principal> principals;

        private final String user;

        private Key(AuthInfo authInfo, String revisionId) {
            this.user = authInfo.getUserID();
            this.principals = authInfo.getPrincipals();
            this.revisionId = revisionId;
        }

        @Override
        public boolean equals(Object object) {
            if (object == null) {
                return false;
            }

            if (object == this) {
                return true;
            }

            if (getClass() != object.getClass()) {
                return false;
            }

            Key other = (Key) object;

            if (!Objects.equal(revisionId, other.revisionId)) {
                return false;
            }

            if (!Objects.equal(user, other.user)) {
                return false;
            }

            if (!Objects.equal(principals, other.principals)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(revisionId, user, principals);
        }

    }

    private Map<Key, Root> roots;

    public ContentRemoteRevisions() {
        this.roots = Maps.newHashMap();
    }

    private Key key(AuthInfo authInfo, String revisionId) {
        return new Key(authInfo, revisionId);
    }

    public Root get(AuthInfo authInfo, String revisionId) {
        return roots.get(key(authInfo, revisionId));
    }

    public String put(AuthInfo authInfo, Root root) {
        String revisionId = UUID.randomUUID().toString();

        roots.put(key(authInfo, revisionId), root);

        return revisionId;
    }

}
