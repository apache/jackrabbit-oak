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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.AccessControlException;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;

/**
 * Aggregates of a collection of {@link RestrictionProvider} implementations
 * into a single provider.
 */
public final class CompositeRestrictionProvider implements RestrictionProvider {

    private final RestrictionProvider[] providers;

    private CompositeRestrictionProvider(@Nonnull Collection<? extends RestrictionProvider> providers) {
        this.providers = providers.toArray(new RestrictionProvider[providers.size()]);
    }

    public static RestrictionProvider newInstance(@Nonnull RestrictionProvider... providers) {
        return newInstance(Arrays.asList(providers));
    }

    public static RestrictionProvider newInstance(@Nonnull Collection<? extends RestrictionProvider> providers) {
        switch (providers.size()) {
            case 0: return EMPTY;
            case 1: return providers.iterator().next();
            default: return new CompositeRestrictionProvider(providers);
        }
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
    public Restriction createRestriction(@Nullable String oakPath, @Nonnull String oakName, @Nonnull Value value) throws RepositoryException {
        return getProvider(oakPath, oakName).createRestriction(oakPath, oakName, value);
    }

    @Nonnull
    @Override
    public Restriction createRestriction(@Nullable String oakPath, @Nonnull String oakName, @Nonnull Value... values) throws RepositoryException {
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
            rp.writeRestrictions(oakPath, aceTree, Collections.singleton(r));
        }
    }

    @Override
    public void validateRestrictions(@Nullable String oakPath, @Nonnull Tree aceTree) throws RepositoryException {
        Map<String,RestrictionDefinition> supported = getSupported(oakPath);
        Set<String> rNames = new HashSet<String>();
        for (Restriction r : readRestrictions(oakPath, aceTree)) {
            String name = getName(r);
            rNames.add(name);
            if (!supported.containsKey(name)) {
                throw new AccessControlException("Unsupported restriction: " + name + " at " + oakPath);
            }
            if (!r.getDefinition().equals(supported.get(name))) {
                throw new AccessControlException("Invalid restriction: " + name + " at " + oakPath);
            }
        }
        for (RestrictionDefinition def : supported.values()) {
            String defName = def.getName();
            if (hasRestrictionProperty(aceTree, defName) && !rNames.contains(defName)) {
                throw new AccessControlException("Invalid restriction " + defName + " at " + oakPath);
            }
            if (def.isMandatory() && !rNames.contains(defName)) {
                throw new AccessControlException("Mandatory restriction " + defName + " is missing.");
            }
        }
    }

    @Nonnull
    @Override
    public RestrictionPattern getPattern(@Nullable String oakPath, @Nonnull Tree tree) {
        return getPattern(oakPath, readRestrictions(oakPath, tree));
    }

    @Nonnull
    @Override
    public RestrictionPattern getPattern(@Nullable String oakPath, @Nonnull Set<Restriction> restrictions) {
        List<RestrictionPattern> patterns = new ArrayList<RestrictionPattern>();
        for (RestrictionProvider rp : providers) {
            RestrictionPattern pattern = rp.getPattern(oakPath, restrictions);
            if (pattern != RestrictionPattern.EMPTY) {
                patterns.add(pattern);
            }
        }
        return CompositePattern.create(patterns);
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

    private Map<String, RestrictionDefinition> getSupported(@Nullable String oakPath) {
        Map<String, RestrictionDefinition> supported = new HashMap<String, RestrictionDefinition>();
        for (RestrictionProvider rp : providers) {
            for (RestrictionDefinition rd : rp.getSupportedRestrictions(oakPath)) {
                supported.put(rd.getName(), rd);
            }
        }
        return supported;
    }

    private static boolean hasRestrictionProperty(Tree aceTree, String name) {
        if (aceTree.hasProperty(name)) {
            return true;
        }
        Tree restrictionTree = aceTree.getChild(AccessControlConstants.REP_RESTRICTIONS);
        return restrictionTree.exists() && restrictionTree.hasProperty(name);
    }

    private static String getName(Restriction restriction) {
        return restriction.getDefinition().getName();
    }
}