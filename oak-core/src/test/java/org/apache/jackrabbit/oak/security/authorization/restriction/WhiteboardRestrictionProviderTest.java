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

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.Value;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WhiteboardRestrictionProviderTest {

    private final Whiteboard whiteboard = new DefaultWhiteboard();

    private final WhiteboardRestrictionProvider restrictionProvider = new WhiteboardRestrictionProvider();

    private final Tree tree = mock(Tree.class);

    private final class RestrictionException extends RuntimeException {}

    private RestrictionProvider registered;

    @Before
    public void before() {
        registered = mock(RestrictionProvider.class);
        when(registered.getPattern(PathUtils.ROOT_PATH, tree)).thenThrow(new RestrictionException());

        restrictionProvider.start(whiteboard);
        whiteboard.register(RestrictionProvider.class, registered, Map.of());
    }

    @After
    public void after() {
        restrictionProvider.stop();
    }

    @Test
    public void testCreateRestriction() throws Exception {
        Value value = mock(Value.class);
        restrictionProvider.createRestriction("/testPath", "name", value);
        restrictionProvider.createRestriction("/testPath", "name", new Value[] {value});

        verify(registered, times(1)).createRestriction("/testPath", "name", value);
        verify(registered, times(1)).createRestriction("/testPath", "name", new Value[] {value});
    }

    @Test
    public void testReadRestrictions() {
        Tree tree = mock(Tree.class);
        restrictionProvider.readRestrictions("/testPath", tree);

        verify(registered, times(1)).readRestrictions("/testPath", tree);
    }

    @Test
    public void testWriteRestrictions() throws Exception {
        Tree tree = mock(Tree.class);
        restrictionProvider.writeRestrictions("/testPath", tree, Set.of());

        verify(registered, times(1)).writeRestrictions("/testPath", tree, Set.of());
    }

    @Test
    public void testValidateRestrictions() throws Exception {
        Tree tree = mock(Tree.class);
        restrictionProvider.validateRestrictions("/testPath", tree);

        verify(registered, times(1)).validateRestrictions("/testPath", tree);
    }

    @Test
    public void testDefaultGetPattern() {
        assertSame(RestrictionPattern.EMPTY, new WhiteboardRestrictionProvider().getPattern(PathUtils.ROOT_PATH, tree));
    }

    @Test
    public void testStartedGetPattern() {
        Whiteboard wb = new DefaultWhiteboard();
        WhiteboardRestrictionProvider wrp = new WhiteboardRestrictionProvider();
        wrp.start(wb);
        assertSame(RestrictionPattern.EMPTY, wrp.getPattern(PathUtils.ROOT_PATH, tree));
    }

    @Test(expected = RestrictionException.class)
    public void testRegisteredGetPattern() {
        registered.getPattern(PathUtils.ROOT_PATH, tree);
    }

    @Test
    public void testGetPatternFromRestrictions() {
        Restriction r = mock(Restriction.class);
        restrictionProvider.getPattern("/testPath", Set.of(r));

        verify(registered, times(1)).getPattern("/testPath", Set.of(r));
    }
}