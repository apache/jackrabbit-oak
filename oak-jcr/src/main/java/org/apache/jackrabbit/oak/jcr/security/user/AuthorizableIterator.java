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
package org.apache.jackrabbit.oak.jcr.security.user;

import java.util.Iterator;
import javax.annotation.Nullable;
import javax.jcr.RangeIterator;
import javax.jcr.RepositoryException;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AuthorizableIterator...
 */
class AuthorizableIterator implements Iterator {

    private static final Logger log = LoggerFactory.getLogger(AuthorizableIterator.class);

    private final Iterator<Authorizable> authorizables;
    private final long size;

    private Authorizable next;

    AuthorizableIterator(Iterator<String> authorizableOakPath, UserManagerImpl userManager) {
        this(authorizableOakPath, userManager, UserManager.SEARCH_TYPE_AUTHORIZABLE);
    }

    AuthorizableIterator(Iterator<String> authorizableOakPaths, UserManagerImpl userManager, int authorizableType) {
        Iterator<Authorizable> it = Iterators.transform(authorizableOakPaths, new ToAuthorizable(userManager, authorizableType));
        this.authorizables = Iterators.filter(it, Predicates.notNull());

        if (authorizableOakPaths instanceof RangeIterator) {
            size = ((RangeIterator) authorizableOakPaths).getSize();
        } else {
            size = -1;
        }
    }

    //-----------------------------------------------------------< Iterator >---
    @Override
    public boolean hasNext() {
        return authorizables.hasNext();
    }

    @Override
    public Authorizable next() {
        return authorizables.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    //--------------------------------------------------------------------------
    long getSize() {
        return size;
    }

    //--------------------------------------------------------------------------

    private static class ToAuthorizable implements Function<String, Authorizable> {

        private final UserManagerImpl userManager;
        private final Predicate predicate;

        public ToAuthorizable(UserManagerImpl userManager, int type) {
            this.userManager = userManager;
            this.predicate = new AuthorizableTypePredicate(type);
        }

        @Override
        public Authorizable apply(@Nullable String oakPath) {
            String jcrPath = userManager.getNamePathMapper().getJcrPath(oakPath);
            try {
                Authorizable a = userManager.getAuthorizableByPath(jcrPath);
                if (predicate.apply(a)) {
                    return a;
                }
            } catch (RepositoryException e) {
                log.debug("Failed to access authorizable " + jcrPath);
            }
            return null;
        }
    }

    private static class AuthorizableTypePredicate implements Predicate<Authorizable> {

        private final int authorizableType;

        AuthorizableTypePredicate(int authorizableType) {
            this.authorizableType = authorizableType;
        }

        @Override
        public boolean apply(Authorizable authorizable) {
            if (authorizable == null) {
                return false;
            }
            switch (authorizableType) {
                case UserManager.SEARCH_TYPE_AUTHORIZABLE:
                    return true;
                case UserManager.SEARCH_TYPE_GROUP:
                    return authorizable.isGroup();
                case UserManager.SEARCH_TYPE_USER:
                    return !authorizable.isGroup();
                default:
                    log.warn("Illegal authorizable type " + authorizableType);
                    return false;
            }
        }
    }
}