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
package org.apache.jackrabbit.oak.security.user;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.LongUtils;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RangeIterator;
import javax.jcr.RepositoryException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

/**
 * AuthorizableIterator...
 */
final class AuthorizableIterator implements Iterator<Authorizable> {

    private static final Logger log = LoggerFactory.getLogger(AuthorizableIterator.class);

    private final Iterator<Authorizable> authorizables;
    private final long size;
    private final Set<String> servedIds;

    @NotNull
    static AuthorizableIterator create(@NotNull Iterator<Tree> authorizableTrees,
                                       @NotNull UserManagerImpl userManager,
                                       @NotNull AuthorizableType authorizableType) {
        Iterator<Authorizable> it = Iterators.transform(authorizableTrees, new TreeToAuthorizable(userManager, authorizableType));
        long size = getSize(authorizableTrees);
        return new AuthorizableIterator(it, size, false);
    }
    
    @NotNull
    static AuthorizableIterator create(boolean filterDuplicates, @NotNull Iterator<? extends Authorizable> it1, @NotNull Iterator<? extends Authorizable> it2) {
        long size = 0;
        for (Iterator<?> it : new Iterator[] {it1, it2}) {
            long l = getSize(it);
            if (l == -1) {
                size = -1;
                break;
            } else {
                size = LongUtils.safeAdd(size, l);
            }
        }
        return new AuthorizableIterator(Iterators.concat(it1, it2), size, filterDuplicates);
    }

    private AuthorizableIterator(Iterator<Authorizable> authorizables, long size, boolean filterDuplicates) {
        if (filterDuplicates)  {
            this.servedIds = new HashSet<>();
            this.authorizables = Iterators.filter(authorizables, authorizable -> {
                if (authorizable == null) {
                    return false;
                }
                String id = Utils.getIdOrNull(authorizable);
                return id != null && servedIds.add(id);
            });
        } else {
            this.servedIds = null;
            this.authorizables = Iterators.filter(authorizables, Objects::nonNull);
        }
        this.size = size;
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

    private static long getSize(Iterator<?> it) {
        if (it instanceof RangeIterator) {
            return ((RangeIterator) it).getSize();
        } else if (it instanceof AuthorizableIterator) {
            return ((AuthorizableIterator) it).getSize();
        } else if (!it.hasNext()) {
            return 0;
        } else {
            return -1;
        }
    }

    private static class TreeToAuthorizable implements Function<Tree, Authorizable> {

        private final UserManagerImpl userManager;
        private final Predicate<Authorizable> predicate;

        TreeToAuthorizable(UserManagerImpl userManager, AuthorizableType type) {
            this.userManager = userManager;
            this.predicate = new AuthorizableTypePredicate(type);
        }

        @Override
        public Authorizable apply(Tree tree) {
            try {
                Authorizable a = userManager.getAuthorizable(tree);
                if (predicate.test(a)) {
                    return a;
                }
            } catch (RepositoryException e) {
                log.debug("Failed to access authorizable {}", tree.getPath());
            }
            return null;
        }
    }

    private static class AuthorizableTypePredicate implements Predicate<Authorizable> {

        private final AuthorizableType authorizableType;

        AuthorizableTypePredicate(@NotNull AuthorizableType authorizableType) {
            this.authorizableType = authorizableType;
        }

        @Override
        public boolean test(Authorizable authorizable) {
            return authorizableType.isType(authorizable);
        }
    }
}