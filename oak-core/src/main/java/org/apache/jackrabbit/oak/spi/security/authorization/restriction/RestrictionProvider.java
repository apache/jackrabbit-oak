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
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.AccessControlException;

import org.apache.jackrabbit.oak.api.Tree;

/**
 * Interface to manage the supported restrictions present with a given access
 * control and permission management implementation.
 *
 * @since OAK 1.0
 */
public interface RestrictionProvider {

    /**
     * Returns the restriction definitions supported by this provider implementation
     * at the specified path.
     *
     * @param oakPath The path of the access controlled tree. A {@code null}
     * path indicates that the supported restrictions for repository level
     * policies should be returned.
     * @return The set of supported restrictions at the given path.
     */
    @Nonnull
    Set<RestrictionDefinition> getSupportedRestrictions(@Nullable String oakPath);

    /**
     * Creates a new single valued restriction for the specified parameters.
     *
     * @param oakPath The path of the access controlled tree or {@code null} if
     * the target policies applies to the repository level.
     * @param oakName The name of the restriction.
     * @param value The value of the restriction.
     * @return A new restriction instance.
     * @throws AccessControlException If no matching restriction definition
     * exists for the specified parameters.
     * @throws RepositoryException If another error occurs.
     */
    @Nonnull
    Restriction createRestriction(@Nullable String oakPath,
                                  @Nonnull String oakName,
                                  @Nonnull Value value) throws AccessControlException, RepositoryException;

    /**
     * Creates a new multi valued restriction for the specified parameters.
     *
     * @param oakPath The path of the access controlled tree or {@code null} if
     * the target policies applies to the repository level.
     * @param oakName The name of the restriction.
     * @param values The values of the restriction.
     * @return A new restriction instance.
     * @throws AccessControlException If no matching restriction definition
     * exists for the specified parameters.
     * @throws RepositoryException If another error occurs.
     */
    @Nonnull
    Restriction createRestriction(@Nullable String oakPath,
                                  @Nonnull String oakName,
                                  @Nonnull Value... values) throws AccessControlException, RepositoryException;

    /**
     * Read the valid restrictions stored in the specified ACE tree.
     *
     * @param oakPath The path of the access controlled tree or {@code null} if
     * the target policies applies to the repository level.
     * @param aceTree The tree corresponding to an ACE that may contain
     * restrictions.
     * @return The valid restrictions stored with the specified tree or an
     * empty set.
     */
    @Nonnull
    Set<Restriction> readRestrictions(@Nullable String oakPath, @Nonnull Tree aceTree);

    /**
     * Writes the given restrictions to the specified ACE tree. Note, that this
     * method does not need to validate the specified restrictions (see also
     * {@link #validateRestrictions(String, org.apache.jackrabbit.oak.api.Tree)}).
     *
     * @param oakPath The path of the access controlled tree or {@code null} if
     * the target policies applies to the repository level.
     * @param aceTree The tree corresponding to an ACE that will have the
     * specified restrictions added.
     * @param restrictions The set of restrictions to be written to the specified
     * tree.
     * @throws RepositoryException If an error occurs while writing the
     * restrictions.
     */
    void writeRestrictions(String oakPath, Tree aceTree, Set<Restriction> restrictions) throws RepositoryException;

    /**
     * Validate the restrictions present with the specified ACE tree.
     *
     * @param oakPath The path of the access controlled tree or {@code null} if
     * the target policies applies to the repository level.
     * @param aceTree The tree corresponding to an ACE.
     * @throws AccessControlException If any invalid restrictions are detected.
     * @throws RepositoryException If another error occurs.
     */
    void validateRestrictions(@Nullable String oakPath, @Nonnull Tree aceTree) throws AccessControlException, RepositoryException;

    /**
     * Creates the {@link RestrictionPattern} for the restriction information
     * stored with specified tree.
     *
     * @param oakPath The path of the access controlled tree or {@code null} if
     * the target policies applies to the repository level.
     * @param tree The tree holding the restriction information.
     * @return A new {@link RestrictionPattern} representing the restriction
     * information present with the given tree.
     */
    @Nonnull
    RestrictionPattern getPattern(@Nullable String oakPath, @Nonnull Tree tree);

    /**
     * Creates the {@link RestrictionPattern} for the specified restrictions.
     * The implementation should ignore all restrictions present in the specified
     * set that it doesn't support.
     *
     * @param oakPath The path of the access controlled tree or {@code null} if
     * the target policies applies to the repository level.
     * @param restrictions the restrictions.
     * @return A new {@link RestrictionPattern} representing those restrictions
     * of the specified set that are supported by this implementation.
     */
    @Nonnull
    RestrictionPattern getPattern(@Nullable String oakPath, @Nonnull Set<Restriction> restrictions);

    /**
     * Empty restriction provider implementation that doesn't support any
     * restrictions.
     */
    RestrictionProvider EMPTY = new RestrictionProvider() {

        @Nonnull
        @Override
        public Set<RestrictionDefinition> getSupportedRestrictions(@Nullable String oakPath) {
            return Collections.emptySet();
        }

        @Nonnull
        @Override
        public Restriction createRestriction(@Nullable String oakPath, @Nonnull String oakName, @Nonnull Value value) throws AccessControlException, RepositoryException {
            throw new AccessControlException("This implementation doesn't support any restrictions");
        }

        @Nonnull
        @Override
        public Restriction createRestriction(@Nullable String oakPath, @Nonnull String oakName, @Nonnull Value... values) throws AccessControlException, RepositoryException {
            throw new AccessControlException("This implementation doesn't support any restrictions");
        }

        @Nonnull
        @Override
        public Set<Restriction> readRestrictions(@Nullable String oakPath, @Nonnull Tree aceTree) {
            return Collections.emptySet();
        }

        @Override
        public void writeRestrictions(String oakPath, Tree aceTree, Set<Restriction> restrictions) {
            // nothing to do
        }

        @Override
        public void validateRestrictions(@Nullable String oakPath, @Nonnull Tree aceTree) {
            // nothing to do
        }

        @Nonnull
        @Override
        public RestrictionPattern getPattern(@Nullable String oakPath, @Nonnull Tree tree) {
            return RestrictionPattern.EMPTY;
        }

        @Nonnull
        @Override
        public RestrictionPattern getPattern(@Nullable String oakPath, @Nonnull Set<Restriction> restrictions) {
            return RestrictionPattern.EMPTY;
        }
    };
}
