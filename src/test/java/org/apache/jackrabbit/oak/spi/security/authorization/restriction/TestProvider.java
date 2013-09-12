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

import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;

/**
 * RestrictionProvider for tests.
 */
final class TestProvider extends AbstractRestrictionProvider {

    TestProvider(Map<String, ? extends RestrictionDefinition> supportedRestrictions) {
        super(supportedRestrictions);
    }

    @Nonnull
    @Override
    public RestrictionPattern getPattern(@Nullable String oakPath, @Nonnull Tree tree) {
        boolean hasRestriction = false;
        for (RestrictionDefinition rd : getSupportedRestrictions(oakPath)) {
            if (tree.hasProperty(rd.getName())) {
                hasRestriction = true;
                break;
            }
        }
        return (hasRestriction) ? new MatchingPattern() : RestrictionPattern.EMPTY;
    }

    private static final class MatchingPattern implements RestrictionPattern {

        @Override
        public boolean matches(@Nonnull Tree tree, @Nullable PropertyState property) {
            return true;
        }

        @Override
        public boolean matches(@Nonnull String path) {
            return true;
        }

        @Override
        public boolean matches() {
            return true;
        }
    }
}