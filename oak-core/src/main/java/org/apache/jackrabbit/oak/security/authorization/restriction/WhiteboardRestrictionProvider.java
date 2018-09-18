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
package org.apache.jackrabbit.oak.security.authorization.restriction;

import java.util.Set;

import javax.jcr.RepositoryException;
import javax.jcr.Value;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.CompositeRestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.AbstractServiceTracker;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Dynamic {@link RestrictionProvider} based on the available
 * whiteboard services.
 */
public class WhiteboardRestrictionProvider
        extends AbstractServiceTracker<RestrictionProvider>
        implements RestrictionProvider {

    public WhiteboardRestrictionProvider() {
        super(RestrictionProvider.class);
    }

    @NotNull
    @Override
    public Set<RestrictionDefinition> getSupportedRestrictions(@Nullable String oakPath) {
        return getProvider().getSupportedRestrictions(oakPath);
    }

    @NotNull
    @Override
    public Restriction createRestriction(@Nullable String oakPath, @NotNull String oakName, @NotNull Value value) throws RepositoryException {
        return getProvider().createRestriction(oakPath, oakName, value);
    }

    @NotNull
    @Override
    public Restriction createRestriction(@Nullable String oakPath, @NotNull String oakName, @NotNull Value... values) throws RepositoryException {
        return getProvider().createRestriction(oakPath, oakName, values);
    }

    @NotNull
    @Override
    public Set<Restriction> readRestrictions(@Nullable String oakPath, @NotNull Tree aceTree) {
        return getProvider().readRestrictions(oakPath, aceTree);
    }

    @Override
    public void writeRestrictions(String oakPath, Tree aceTree, Set<Restriction> restrictions) throws RepositoryException {
        getProvider().writeRestrictions(oakPath, aceTree, restrictions);
    }

    @Override
    public void validateRestrictions(@Nullable String oakPath, @NotNull Tree aceTree) throws RepositoryException {
        getProvider().validateRestrictions(oakPath, aceTree);
    }

    @NotNull
    @Override
    public RestrictionPattern getPattern(@Nullable String oakPath, @NotNull Tree tree) {
        return getProvider().getPattern(oakPath, tree);
    }

    @NotNull
    @Override
    public RestrictionPattern getPattern(@Nullable String oakPath, @NotNull Set<Restriction> restrictions) {
        return getProvider().getPattern(oakPath, restrictions);
    }

    //------------------------------------------------------------< private >---
    private RestrictionProvider getProvider() {
        return CompositeRestrictionProvider.newInstance(getServices());
    }
}
