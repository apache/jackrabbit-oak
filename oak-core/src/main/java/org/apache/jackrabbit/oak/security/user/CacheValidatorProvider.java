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
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.commit.VisibleValidator;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Validator provider to ensure that the principal-cache stored with a given
 * user is only maintained by the {@link org.apache.jackrabbit.oak.security.user.UserPrincipalProvider}
 * associated with a internal system session.
 */
class CacheValidatorProvider extends ValidatorProvider implements CacheConstants {

    private final boolean isSystem;
    private final TreeProvider treeProvider;

    CacheValidatorProvider(@Nonnull Set<Principal> principals, @Nonnull TreeProvider treeProvider) {
        super();
        isSystem = principals.contains(SystemPrincipal.INSTANCE);
        this.treeProvider = treeProvider;
    }

    @CheckForNull
    @Override
    protected Validator getRootValidator(NodeState before, NodeState after, CommitInfo info) {
        TypePredicate cachePredicate = new TypePredicate(after, NT_REP_CACHE);
        boolean isValidCommitInfo = CommitMarker.isValidCommitInfo(info);
        return new CacheValidator(treeProvider.createReadOnlyTree(before), treeProvider.createReadOnlyTree(after), cachePredicate, isValidCommitInfo);
    }

    //--------------------------------------------------------------------------

    static Map<String, Object> asCommitAttributes() {
        return Collections.<String, Object>singletonMap(CommitMarker.KEY, CommitMarker.INSTANCE);
    }

    private static final class CommitMarker {

        private static final String KEY = CommitMarker.class.getName();

        private static final CommitMarker INSTANCE = new CommitMarker();

        private static boolean isValidCommitInfo(@Nonnull CommitInfo commitInfo) {
            return CommitMarker.INSTANCE == commitInfo.getInfo().get(CommitMarker.KEY);
        }

        private CommitMarker() {}
    }

    private static CommitFailedException constraintViolation(int code, @Nonnull String message) {
        return new CommitFailedException(CommitFailedException.CONSTRAINT, code, message);
    }

    //-----------------------------------------------------< CacheValidator >---
    private final class CacheValidator extends DefaultValidator {

        private final Tree parentBefore;
        private final Tree parentAfter;

        private final TypePredicate cachePredicate;
        private final boolean isValidCommitInfo;

        private final boolean isCache;

        private CacheValidator(@Nullable Tree parentBefore, @Nonnull Tree parentAfter, TypePredicate cachePredicate, boolean isValidCommitInfo) {
            this.parentBefore = parentBefore;
            this.parentAfter = parentAfter;

            this.cachePredicate = cachePredicate;
            this.isValidCommitInfo = isValidCommitInfo;

            isCache = isCache(parentAfter);
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
            Tree beforeTree = (parentBefore == null) ? null : parentBefore.getChild(name);
            Tree afterTree = parentAfter.getChild(name);

            if (isCache || isCache(beforeTree) || isCache(afterTree)) {
                checkValidCommit();
            }

            return new VisibleValidator(new CacheValidator(beforeTree, afterTree, cachePredicate, isValidCommitInfo), true, true);
        }

        @Override
        public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
            Tree tree = checkNotNull(parentAfter.getChild(name));
            if (isCache || isCache(tree)) {
                checkValidCommit();
            }
            return new VisibleValidator(new CacheValidator(null, tree, cachePredicate, isValidCommitInfo), true, true);
        }

        private boolean isCache(@CheckForNull Tree tree) {
            return tree != null && (REP_CACHE.equals(tree.getName()) || cachePredicate.apply(tree));
        }

        private void checkValidCommit() throws CommitFailedException {
            if (!(isSystem && isValidCommitInfo)) {
                throw constraintViolation(34, "Attempt to create or change the system maintained cache.");
            }
        }
    }
}