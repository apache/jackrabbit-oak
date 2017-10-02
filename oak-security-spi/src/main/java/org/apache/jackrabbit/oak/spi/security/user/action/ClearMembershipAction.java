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
package org.apache.jackrabbit.oak.spi.security.user.action;

import java.util.Iterator;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;

/**
 * Authorizable action attempting to clear all group membership before removing
 * the specified authorizable. If {@link Group#removeMember(Authorizable)}
 * fails due to lack of permissions {@link AuthorizableAction#onRemove(org.apache.jackrabbit.api.security.user.Authorizable, org.apache.jackrabbit.oak.api.Root, org.apache.jackrabbit.oak.namepath.NamePathMapper)}
 * throws an exception and removing the specified authorizable will be aborted.
 */
public class ClearMembershipAction extends AbstractAuthorizableAction {

    //-------------------------------------------------< AuthorizableAction >---
    @Override
    public void onRemove(@Nonnull Authorizable authorizable, @Nonnull Root root, @Nonnull NamePathMapper namePathMapper) throws RepositoryException {
        clearMembership(authorizable);
    }

    //--------------------------------------------------------------------------
    private static void clearMembership(Authorizable authorizable) throws RepositoryException {
        Iterator<Group> membership = authorizable.declaredMemberOf();
        while (membership.hasNext()) {
            membership.next().removeMember(authorizable);
        }
    }
}
