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
package org.apache.jackrabbit.api.security.authorization;

import org.jetbrains.annotations.NotNull;

import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

/**
 * <p>Wrapper around a set of {@link Privilege}s that allows to test if a given list of privilege names in included. This 
 * avoids repeated calls to {@link AccessControlManager#hasPrivileges(String, Privilege[])} or having to manually resolve 
 * the privilege aggregation when using {@link AccessControlManager#getPrivileges(String)}.</p>
 * 
 * While a {@link PrivilegeCollection.Default default} is available for backwards compatibility, it uses regular 
 * JCR API. Therefore it is recommended to provide custom implementations of 
 * {@link org.apache.jackrabbit.api.security.JackrabbitAccessControlManager#getPrivilegeCollection(String)} and 
 * {@link org.apache.jackrabbit.api.security.JackrabbitAccessControlManager#getPrivilegeCollection(String, Set)} with 
 * efficient implementations of the {@code PrivilegeCollection}.
 * 
 * @since Oak 1.42.0
 * @see org.apache.jackrabbit.api.security.JackrabbitAccessControlManager#getPrivilegeCollection(String) 
 * @see org.apache.jackrabbit.api.security.JackrabbitAccessControlManager#getPrivilegeCollection(String, Set) 
 */
public interface PrivilegeCollection {

    /**
     * Return the underlying privilege array.
     * 
     * @return the privilege array for which this instance has been created.
     * @throws RepositoryException If an error occurs.
     */
    Privilege[] getPrivileges() throws RepositoryException;

    /**
     * Tests whether the given JCR {@code privilegeNames} are contained in the privileges for which this instance of 
     * {@code PrivilegeEvaluation} has been created such as e.g. through 
     * {@link org.apache.jackrabbit.api.security.JackrabbitAccessControlManager#getPrivilegeCollection(String)} or  
     * {@link org.apache.jackrabbit.api.security.JackrabbitAccessControlManager#getPrivilegeCollection(String, Set)}. 
     * The inclusion can either be direct or through privilege aggregation.
     * 
     * @param privilegeNames The JCR names of privileges to be tested. They can be passed in expanded form 
     * (like e.g. {@link Privilege#JCR_READ}) or in qualified form (i.e. 'jcr:read' if 'jcr' was the prefixed defined for 
     * the 'http://www.jcp.org/jcr/1.0' namespace.
     * @return {@code true} if the underlying {@code privileges} include all specified privilege names either directly 
     * or by means of aggregation; {@code false} if one or multiple privileges are not included. If {@code jcr:all} privilege 
     * is part of this collection or if no privilege names are specified this method will return {@code true}. 
     * If no privileges are granted {@code false} is returned.
     * @throws AccessControlException If any of the given privilege names is invalid i.e. no such privilege exists.
     * @throws RepositoryException If another error occurs.
     * @since Oak 1.42.0
     */
    boolean includes(@NotNull String... privilegeNames) throws RepositoryException;

    /**
     * Default implementation of the {@link PrivilegeCollection} interface.
     * 
     * Note that this implementation uses JCR API to resolve privilege aggregation, which is expected to be inefficient.
     * Implementations of {@link org.apache.jackrabbit.api.security.JackrabbitAccessControlManager#getPrivilegeCollection(String)} 
     * and {@link org.apache.jackrabbit.api.security.JackrabbitAccessControlManager#getPrivilegeCollection(String, Set)} 
     * therefore should provide improved implementations of the {@code PrivilegeCollection} interface.
     * 
     * @since Oak 1.42.0
     */
    class Default implements PrivilegeCollection {
        
        private final Privilege[] privileges;
        private final AccessControlManager accessControlManager;
        
        public Default(@NotNull Privilege[] privileges, @NotNull AccessControlManager accessControlManager) {
            this.privileges = privileges;
            this.accessControlManager = accessControlManager;
        }

        @Override
        public Privilege[] getPrivileges() {
            return privileges;
        }

        @Override
        public boolean includes(@NotNull String... privilegeNames) throws RepositoryException {
            if (privilegeNames.length == 0) {
                return true;
            }
            if (privileges.length == 0) {
                return false;
            }
            Set<Privilege> toTest = new HashSet<>(privilegeNames.length);
            for (String pName : privilegeNames) {
                toTest.add(accessControlManager.privilegeFromName(pName));
            }
            Set<Privilege> privilegeSet = new HashSet<>(Arrays.asList(privileges));
            if (privilegeSet.containsAll(toTest)) {
                return true;
            } else {
                Stream.of(privileges).filter(Privilege::isAggregate).forEach(privilege -> Collections.addAll(privilegeSet, privilege.getAggregatePrivileges()));
                return privilegeSet.containsAll(toTest);
            }
        }
    }
}
