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

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CompositePatternTest {

    private final RestrictionPattern alwaysMatching = CompositePattern.create(ImmutableList.of(TestRestrictionPatter.INSTANCE_TRUE, TestRestrictionPatter.INSTANCE_TRUE));
    private final RestrictionPattern neverMatching = CompositePattern.create(ImmutableList.of(TestRestrictionPatter.INSTANCE_TRUE, TestRestrictionPatter.INSTANCE_FALSE));

    @Test
    public void testCreateFromEmptyList() {
        RestrictionPattern rp = CompositePattern.create(ImmutableList.<RestrictionPattern>of());
        assertSame(RestrictionPattern.EMPTY, rp);
    }

    @Test
    public void testCreateFromSingletonList() {
        RestrictionPattern rp = CompositePattern.create(ImmutableList.of(TestRestrictionPatter.INSTANCE_TRUE));
        assertSame(TestRestrictionPatter.INSTANCE_TRUE, rp);
    }

    @Test
    public void testCreateFromList() {
        RestrictionPattern rp = CompositePattern.create(ImmutableList.of(TestRestrictionPatter.INSTANCE_TRUE, TestRestrictionPatter.INSTANCE_FALSE));
        assertTrue(rp instanceof CompositePattern);
    }

    @Test
    public void testMatches() {
        assertTrue(alwaysMatching.matches());
        assertFalse(neverMatching.matches());
    }

    @Test
    public void testMatchesPath() {
        List<String> paths = ImmutableList.of("/", "/a", "/a/b/c", "");

        for (String path : paths) {
            assertTrue(alwaysMatching.matches(path));
            assertFalse(neverMatching.matches(path));
        }
    }

    @Test
    public void testMatchesTree() {
        Tree tree = Mockito.mock(Tree.class);

        assertTrue(alwaysMatching.matches(tree, null));
        assertFalse(neverMatching.matches(tree, null));
    }

    @Test
    public void testMatchesTreeProperty() {
        Tree tree = Mockito.mock(Tree.class);
        PropertyState property = PropertyStates.createProperty("prop", "value");

        assertTrue(alwaysMatching.matches(tree, property));
        assertFalse(neverMatching.matches(tree, property));
    }

    private static final class TestRestrictionPatter implements RestrictionPattern {

        private static RestrictionPattern INSTANCE_TRUE = new TestRestrictionPatter(true);
        private static RestrictionPattern INSTANCE_FALSE = new TestRestrictionPatter(false);

        private final boolean matches;

        private TestRestrictionPatter(boolean matches) {
            this.matches = matches;
        }

        @Override
        public boolean matches(@Nonnull Tree tree, @Nullable PropertyState property) {
            return matches;
        }

        @Override
        public boolean matches(@Nonnull String path) {
            return matches;
        }

        @Override
        public boolean matches() {
            return matches;
        }
    }
}