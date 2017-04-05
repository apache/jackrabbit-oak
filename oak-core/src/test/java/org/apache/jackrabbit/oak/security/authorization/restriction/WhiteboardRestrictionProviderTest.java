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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.AccessControlException;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableNodeName;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.when;

public class WhiteboardRestrictionProviderTest {

    private final Whiteboard whiteboard = new DefaultWhiteboard();

    private final WhiteboardRestrictionProvider restrictionProvider = new WhiteboardRestrictionProvider();

    private final Tree tree = Mockito.mock(Tree.class);

    private final class RestrictionException extends RuntimeException {}

    private RestrictionProvider registered;

    @Before
    public void before() {
        registered = Mockito.mock(RestrictionProvider.class);
        when(registered.getPattern(PathUtils.ROOT_PATH, tree)).thenThrow(new RestrictionException());
    }

    @After
    public void after() {
        restrictionProvider.stop();
    }

    @Test
    public void testDefaultGetPattern() {
        assertSame(RestrictionPattern.EMPTY, restrictionProvider.getPattern(PathUtils.ROOT_PATH, tree));
    }

    @Test
    public void testStartedGetPattern() {
        restrictionProvider.start(whiteboard);
        assertSame(RestrictionPattern.EMPTY, restrictionProvider.getPattern(PathUtils.ROOT_PATH, tree));
    }

    @Test(expected = RestrictionException.class)
    public void testRegisteredGetPattern() {
        restrictionProvider.start(whiteboard);
        whiteboard.register(RestrictionProvider.class, registered, ImmutableMap.of());

        registered.getPattern(PathUtils.ROOT_PATH, tree);
    }
}