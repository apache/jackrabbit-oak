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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertTrue;

public class EmptyPatternTest {

    @Test
    public void testMatches() {
        assertTrue(RestrictionPattern.EMPTY.matches());
    }

    @Test
    public void testMatchesPath() {
        assertTrue(RestrictionPattern.EMPTY.matches("/any/path"));
    }

    @Test
    public void testMatchesTree() {
        assertTrue(RestrictionPattern.EMPTY.matches(Mockito.mock(Tree.class), null));
    }

    @Test
    public void testMatchesProperty() {
        assertTrue(RestrictionPattern.EMPTY.matches(Mockito.mock(Tree.class), Mockito.mock(PropertyState.class)));
    }

}