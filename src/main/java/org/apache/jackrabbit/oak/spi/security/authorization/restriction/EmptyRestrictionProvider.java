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
package org.apache.jackrabbit.oak.spi.security.authorization.restriction;

import java.util.Collections;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.AccessControlException;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.oak.api.Tree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DefaultRestrictionProvider... TODO
 */
public class EmptyRestrictionProvider implements RestrictionProvider {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(EmptyRestrictionProvider.class);

    @Nonnull
    @Override
    public Set<RestrictionDefinition> getSupportedRestrictions(String jcrPath) {
        return Collections.emptySet();
    }

    @Nonnull
    @Override
    public Restriction createRestriction(String jcrPath, String jcrName, Value value) throws RepositoryException {
        throw new AccessControlException("Unsupported restriction " + jcrName);
    }

    @Nonnull
    @Override
    public Set<Restriction> readRestrictions(String jcrPath, Tree aceTree) throws AccessControlException {
        return Collections.emptySet();
    }

    @Override
    public void writeRestrictions(String jcrPath, Tree aceTree, Set<Restriction> restrictions) throws AccessControlException {
        if (!restrictions.isEmpty()) {
            throw new AccessControlException("Implementation doesn't supported restrictions.");
        }
    }

    @Override
    public void writeRestrictions(String jcrPath, Tree aceTree, JackrabbitAccessControlEntry ace) throws AccessControlException {
        throw new AccessControlException("Implementation doesn't supported restrictions.");
    }

    @Override
    public void validateRestrictions(String jcrPath, Tree aceTree) throws AccessControlException {
        // nothing to do.
    }
}