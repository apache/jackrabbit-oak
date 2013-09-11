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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.AccessControlException;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.Tree;

/**
 * Aggregates of a collection of {@link RestrictionProvider} implementations
 * into a single provider.
 */
public class CompositeRestrictionProvider implements RestrictionProvider {

    private final Collection<? extends RestrictionProvider> providers;

    private CompositeRestrictionProvider(Collection<? extends RestrictionProvider> providers) {
        this.providers = providers;
    }

    public static RestrictionProvider newInstance(Collection<? extends RestrictionProvider> providers) {
        return new CompositeRestrictionProvider(providers);
    }

    @Nonnull
    @Override
    public Set<RestrictionDefinition> getSupportedRestrictions(@Nullable String oakPath) {
        Set<RestrictionDefinition> defs = Sets.newHashSet();
        for (RestrictionProvider rp : providers) {
            defs.addAll(rp.getSupportedRestrictions(oakPath));
        }
        return defs;
    }

    @Nonnull
    @Override
    public Restriction createRestriction(@Nullable String oakPath, @Nonnull String oakName, @Nonnull Value value) throws AccessControlException, RepositoryException {
        return getProvider(oakPath, oakName).createRestriction(oakPath, oakName, value);
    }

    @Nonnull
    @Override
    public Restriction createRestriction(@Nullable String oakPath, @Nonnull String oakName, @Nonnull Value... values) throws AccessControlException, RepositoryException {
        return getProvider(oakPath, oakName).createRestriction(oakPath, oakName, values);
    }

    @Nonnull
    @Override
    public Set<Restriction> readRestrictions(@Nullable String oakPath, @Nonnull Tree aceTree) {
        Set<Restriction> restrictions = Sets.newHashSet();
        for (RestrictionProvider rp : providers) {
            restrictions.addAll(rp.readRestrictions(oakPath, aceTree));
        }
        return restrictions;
    }

    @Override
    public void writeRestrictions(String oakPath, Tree aceTree, Set<Restriction> restrictions) throws RepositoryException {
        for (Restriction r : restrictions) {
            RestrictionProvider rp = getProvider(oakPath, getName(r));
            rp.writeRestrictions(oakPath, aceTree, restrictions);
        }
    }

    @Override
    public void validateRestrictions(@Nullable String oakPath, @Nonnull Tree aceTree) throws AccessControlException, RepositoryException {
        Set<RestrictionDefinition> supported = getSupportedRestrictions(oakPath);
        Set<String> rNames = new HashSet<String>();
        for (Restriction r : readRestrictions(oakPath, aceTree)) {
            String name = getName(r);
            rNames.add(name);
            boolean valid = false;
            for (RestrictionDefinition def : supported) {
                if (name.equals(def.getName())) {
                    valid = def.equals(r.getDefinition());
                    break;
                }
            }
            if (!valid) {
                throw new AccessControlException("Invalid restriction: " + r + " at " + oakPath);
            }
        }
        for (RestrictionDefinition def : supported) {
            if (def.isMandatory() && !rNames.contains(def.getName())) {
                throw new AccessControlException("Mandatory restriction " + def.getName() + " is missing.");
            }
        }
    }

    @Nonnull
    @Override
    public RestrictionPattern getPattern(@Nullable String oakPath, @Nonnull Tree tree) {
        List<RestrictionPattern> patterns = new ArrayList<RestrictionPattern>();
        for (RestrictionProvider rp : providers) {
            RestrictionPattern pattern = rp.getPattern(oakPath, tree);
            if (pattern != RestrictionPattern.EMPTY) {
                patterns.add(pattern);
            }
        }
        switch (patterns.size()) {
            case 0 : return RestrictionPattern.EMPTY;
            case 1 : return patterns.iterator().next();
            default : return new CompositePattern(patterns);
        }
    }

    //------------------------------------------------------------< private >---
    private RestrictionProvider getProvider(@Nullable String oakPath, @Nonnull String oakName) throws AccessControlException {
        for (RestrictionProvider rp : providers) {
            for (RestrictionDefinition def : rp.getSupportedRestrictions(oakPath)) {
                if (def.getName().equals(oakName)) {
                    return rp;
                }
            }
        }
        throw new AccessControlException("Unsupported restriction (path = " + oakPath + "; name = " + oakName + ')');
    }

    private static String getName(Restriction restriction) {
        return restriction.getDefinition().getName();
    }
}