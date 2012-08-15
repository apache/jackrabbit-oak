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
package org.apache.jackrabbit.oak.security.principal;

import java.security.Principal;
import java.security.acl.Group;
import java.util.Collections;
import java.util.Set;

import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code KernelPrincipalProvider} is a principal provider implementation
 * that operates on principal information read from user information stored
 * in the{@code MicroKernel}.
 */
public class KernelPrincipalProvider implements PrincipalProvider {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(KernelPrincipalProvider.class);

    //--------------------------------------------------< PrincipalProvider >---
    @Override
    public Principal getPrincipal(final String principalName) {
        // TODO: use user-defined query to search for a principalName property
        // TODO  that is defined by a user/group node.
        return new Principal() {
            @Override
            public String getName() {
                return principalName;
            }
        };
    }

    @Override
    public Set<Group> getGroupMembership(Principal principal) {
        // TODO
        return Collections.<Group>singleton(EveryonePrincipal.getInstance());
    }
}