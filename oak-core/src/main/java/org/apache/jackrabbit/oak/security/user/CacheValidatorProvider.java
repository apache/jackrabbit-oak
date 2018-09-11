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

import java.security.Principal;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.commit.VisibleValidator;
import org.apache.jackrabbit.oak.spi.nodetype.predicate.TypePredicates;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Validator provider to ensure that the principal-cache stored with a given
 * user is only maintained by the {@link org.apache.jackrabbit.oak.security.user.UserPrincipalProvider}
 * associated with a internal system session.
 */
class CacheValidatorProvider extends ValidatorProvider implements CacheConstants {

    private final boolean isSystem;

    CacheValidatorProvider(@NotNull Set<Principal> principals) {
        isSystem = principals.contains(SystemPrincipal.INSTANCE);
    }

    @Nullable
    @Override
    protected Validator getRootValidator(NodeState before, NodeState after, CommitInfo info) {
        Predicate<NodeState> cachePredicate = TypePredicates.getNodeTypePredicate(after, NT_REP_CACHE);
        boolean isValidCommitInfo = CommitMarker.isValidCommitInfo(info);
        return new CacheValidator(before, after, PathUtils.ROOT_NAME, cachePredicate,  isValidCommitInfo);
    }

    //--------------------------------------------------------------------------

    static Map<String, Object> asCommitAttributes() {
        return Collections.<String, Object>singletonMap(CommitMarker.KEY, CommitMarker.INSTANCE);
    }

    private static final class CommitMarker {

        private static final String KEY = CommitMarker.class.getName();

        private static final CommitMarker INSTANCE = new CommitMarker();

        private static boolean isValidCommitInfo(@NotNull CommitInfo commitInfo) {
            return CommitMarker.INSTANCE == commitInfo.getInfo().get(CommitMarker.KEY);
        }

        private CommitMarker() {}
    }

    private static CommitFailedException constraintViolation(int code, @NotNull String message) {
        return new CommitFailedException(CommitFailedException.CONSTRAINT, code, message);
    }

    //-----------------------------------------------------< CacheValidator >---
    private final class CacheValidator extends DefaultValidator {

        private final Predicate<NodeState> cachePredicate;
        private final boolean isValidCommitInfo;

        private final boolean isCache;

        private CacheValidator(@Nullable NodeState before, @NotNull NodeState after, String name, Predicate<NodeState> cachePredicate, boolean isValidCommitInfo) {
            this.cachePredicate = cachePredicate;
            this.isValidCommitInfo = isValidCommitInfo;

            isCache = isCache(after, name);
        }

        @Override
        public void propertyAdded(PropertyState after) throws CommitFailedException {
            if (isCache) {
                checkValidCommit();
            }
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
            if (isCache) {
                checkValidCommit();
            }
        }

        @Override
        public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
            if (isCache || isCache(before, name) || isCache(after, name)) {
                checkValidCommit();
            }
            return new VisibleValidator(new CacheValidator(before, after, name, cachePredicate, isValidCommitInfo), true, true);
        }

        @Override
        public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
            if (isCache || isCache(after, name)) {
                checkValidCommit();
            }
            return new VisibleValidator(new CacheValidator(null, after, name, cachePredicate, isValidCommitInfo), true, true);
        }

        private boolean isCache(@Nullable NodeState state, String name) {
            return state != null && (REP_CACHE.equals(name) || cachePredicate.test(state));
        }

        private void checkValidCommit() throws CommitFailedException {
            if (!(isSystem && isValidCommitInfo)) {
                throw constraintViolation(34, "Attempt to create or change the system maintained cache.");
            }
        }
    }
}
