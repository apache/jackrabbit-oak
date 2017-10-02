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

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;

/**
 * Aggregates of a list of {@link RestrictionPattern}s into a single pattern.
 * The implementations of {@code matches} returns {@code true} if all aggregated
 * patterns successfully validate the given parameters and returns {@code false}
 * as soon as the first aggregated pattern returns {@code false}.
 */
public final class CompositePattern implements RestrictionPattern {

    private final List<RestrictionPattern> patterns;

    public CompositePattern(@Nonnull List<RestrictionPattern> patterns) {
        this.patterns = patterns;
    }

    public static RestrictionPattern create(@Nonnull List<RestrictionPattern> patterns) {
        switch (patterns.size()) {
            case 0 : return RestrictionPattern.EMPTY;
            case 1 : return patterns.get(0);
            default : return new CompositePattern(patterns);
        }
    }

    @Override
    public boolean matches(@Nonnull Tree tree, @Nullable PropertyState property) {
        for (RestrictionPattern pattern : patterns) {
            if (!pattern.matches(tree, property)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean matches(@Nonnull String path) {
        for (RestrictionPattern pattern : patterns) {
            if (!pattern.matches(path)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean matches() {
        for (RestrictionPattern pattern : patterns) {
            if (!pattern.matches()) {
                return false;
            }
        }
        return true;
    }
}