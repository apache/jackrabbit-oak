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
package org.apache.jackrabbit.oak.spi.security.authorization.cug;

import java.security.Principal;
import java.util.Set;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.SystemUserPrincipal;
import org.osgi.annotation.versioning.ProviderType;

/**
 * Interface that allows to exclude certain principals from the CUG evaluation.
 * For the excluded principals the closed user group policies will be ignored.
 */
@ProviderType
public interface CugExclude {

    boolean isExcluded(@Nonnull Set<Principal> principals);

    /**
     * Default implementation of the {@link CugExclude} interface that excludes
     * the following principal classes from CUG evaluation:
     * <ul>
     *     <li>{@link org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal AdminPrincipals}</li>
     *     <li>{@link org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal SystemPrincipal}</li>
     *     <li>{@link org.apache.jackrabbit.oak.spi.security.principal.SystemUserPrincipal SystemUserPrincipal}</li>
     * </ul>
     */
    class Default implements CugExclude {

        @Override
        public boolean isExcluded(@Nonnull Set<Principal> principals) {
            if (principals.isEmpty()) {
                return true;
            }
            if (principals.contains(SystemPrincipal.INSTANCE)) {
                return true;
            }
            for (Principal p : principals) {
                if (p instanceof AdminPrincipal || p instanceof SystemUserPrincipal) {
                    return true;
                }
            }
            return false;
        }
    }
}