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

import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.osgi.annotation.versioning.ProviderType;

/**
 * Extension for the {@code PrincipalManager} that offers range search.
 *
 */
@ProviderType
public interface PrincipalQueryManager {

    /**
     * Gets the principals matching a simple filter expression applied against
     * the {@link Principal#getName() principal name} AND the specified search
     * type.
     * Results are expected to be sorted by the principal name.
     * An implementation may limit the number of principals returned.
     * If there are no matching principals, an empty iterator is returned.
     * @param simpleFilter
     * @param fullText
     * @param searchType Any of the following constants:
     * <ul>
     * <li>{@link PrincipalManager#SEARCH_TYPE_ALL}</li>
     * <li>{@link PrincipalManager#SEARCH_TYPE_GROUP}</li>
     * <li>{@link PrincipalManager#SEARCH_TYPE_NOT_GROUP}</li>
     * </ul>
     * @param offset Offset from where to start returning results. <code>0</code> for no offset.
     * @param limit Maximal number of results to return. -1 for no limit.
     * @return a <code>PrincipalIterator</code> over the <code>Principal</code>s
     * matching the given filter and search type.
     */
    PrincipalIterator findPrincipals(String simpleFilter, boolean fullText, int searchType, long offset, long limit);
}
