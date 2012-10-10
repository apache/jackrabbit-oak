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
package org.apache.jackrabbit.oak.spi.security.principal;

import java.security.Principal;
import java.security.acl.Group;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * OpenPrincipalProvider... TODO
 */
public class OpenPrincipalProvider implements PrincipalProvider {

    //--------------------------------------------------< PrincipalProvider >---
    @Override
    public Principal getPrincipal(final String principalName) {
        return new Principal() {
            @Override
            public String getName() {
                return principalName;
            }
        };
    }

    @Override
    public Set<Group> getGroupMembership(Principal principal) {
        return Collections.<Group>singleton(EveryonePrincipal.getInstance());
    }

    @Override
    public Set<Principal> getPrincipals(String userID) {
        Set<Principal> principals = new HashSet<Principal>();
        Principal p = getPrincipal(userID);
        principals.add(p);
        principals.addAll(getGroupMembership(p));
        return principals;
    }

    @Override
    public Iterator<Principal> findPrincipals(String nameHint, int searchType) {
        return PrincipalIteratorAdapter.EMPTY;
    }
}