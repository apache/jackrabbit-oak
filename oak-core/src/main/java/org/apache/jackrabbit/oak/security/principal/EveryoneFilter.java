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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.security.Principal;
import java.util.Iterator;

public final class EveryoneFilter {
    
    private EveryoneFilter() {
    }

    /**
     * Inject the everyone principal in the result if the given query (defined by name hint and search type) matched 
     * everyone and the search expects the complete set to be return (i.e. no offset and no limit defined).
     * 
     * @param resultPrincipals The principals returned by the query.
     * @param nameHint The original name hint as passed to the query.
     * @param searchType The original search type as passed to the query.
     * @param offset The offset as passed to the query.
     * @param limit The limit as passed to the query.
     * @return The principal iterator with additionally {@link EveryonePrincipal} attached if the given query matches the 
     * everyone principal name or search-type and doesn't specify a range (offset or limit). If the query doesn't match 
     * everyone or if a range was defined the original iterator is returned unmodified.
     */
    public static Iterator<Principal> filter(@NotNull Iterator<Principal> resultPrincipals, @Nullable String nameHint, int searchType, long offset, long limit) {
        boolean noRange = offset == 0 && limit == Long.MAX_VALUE;
        if (noRange && matchesEveryone(nameHint, searchType)) {
            Iterator<Principal> principals = Iterators.concat(resultPrincipals, Iterators.singletonIterator(EveryonePrincipal.getInstance()));
            return Iterators.filter(principals, new EveryonePredicate());
        } else {
            return resultPrincipals;
        }
    }
    
    private static boolean matchesEveryone(@Nullable String nameHint, int searchType) {
        return searchType != PrincipalManager.SEARCH_TYPE_NOT_GROUP &&
                (nameHint == null || EveryonePrincipal.NAME.contains(nameHint));
    }
    
    /**
     * Predicate to make sure the everyone principal is only included once in
     * the result set.
     */
    private static final class EveryonePredicate implements Predicate<Principal> {
        private boolean servedEveryone = false;

        @Override
        public boolean apply(@Nullable Principal principal) {
            if (principal != null && EveryonePrincipal.NAME.equals(principal.getName())) {
                if (servedEveryone) {
                    return false;
                } else {
                    servedEveryone = true;
                    return true;
                }
            } else {
                // not everyone
                return true;
            }
        }
    }
}