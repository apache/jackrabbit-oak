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
package org.apache.jackrabbit.oak.spi.security;

import java.lang.reflect.Field;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CompositeContextTest extends AbstractCompositeConfigurationTest {

    @Before
    public void before() throws Exception {
        compositeConfiguration = new CompositeConfiguration("test", Mockito.mock(SecurityProvider.class)) {};
    }

    @Test
    public void testGetContext() throws Exception {
        Class cls = Class.forName(CompositeConfiguration.class.getName() + "$CompositeContext");
        Field def = cls.getDeclaredField("defaultCtx");
        def.setAccessible(true);

        Field delegatees = cls.getDeclaredField("delegatees");
        delegatees.setAccessible(true);

        Context ctx = compositeConfiguration.getContext();
        assertSame(cls, ctx.getClass());
        assertNull(delegatees.get(ctx));
        assertSame(Context.DEFAULT, def.get(ctx));

        SecurityConfiguration sc = new TestConfiguration();
        setDefault(sc);
        ctx = compositeConfiguration.getContext();
        assertNull(delegatees.get(ctx));
        assertSame(sc.getContext(), def.get(ctx));
        assertSame(cls, ctx.getClass());

        addConfiguration(sc);
        ctx = compositeConfiguration.getContext();
        assertNotSame(sc.getContext(), ctx);
        assertEquals(1, ((Context[]) delegatees.get(ctx)).length);

        // add configuration that has DEFAULT ctx -> must not be added
        SecurityConfiguration defConfig = new SecurityConfiguration.Default();
        addConfiguration(defConfig);
        assertEquals(1, ((Context[]) delegatees.get(compositeConfiguration.getContext())).length);

        // add same test configuration again -> no duplicate entries
        addConfiguration(sc);
        assertEquals(1, ((Context[]) delegatees.get(compositeConfiguration.getContext())).length);

        SecurityConfiguration sc2 = new TestConfiguration();
        addConfiguration(sc2);
        assertEquals(2, ((Context[]) delegatees.get(compositeConfiguration.getContext())).length);

        removeConfiguration(sc2);
        assertEquals(1, ((Context[]) delegatees.get(compositeConfiguration.getContext())).length);

        removeConfiguration(sc);
        removeConfiguration(sc);
        removeConfiguration(defConfig);
        assertNull(delegatees.get(compositeConfiguration.getContext()));
    }

    @Test
    public void testEmpty() {
        Context ctx = compositeConfiguration.getContext();

        assertNotNull(ctx);

        Tree tree = Mockito.mock(Tree.class);
        assertFalse(ctx.definesContextRoot(tree));
        assertFalse(ctx.definesInternal(tree));
        assertFalse(ctx.definesTree(tree));
        assertFalse(ctx.definesProperty(tree, Mockito.mock(PropertyState.class)));
        assertFalse(ctx.definesLocation(TreeLocation.create(tree)));
    }

    @Test
    public void testDefinesProperty() {
        TestConfiguration testConfig = new TestConfiguration(true);
        addConfiguration(testConfig);

        assertTrue(compositeConfiguration.getContext().definesProperty(Mockito.mock(Tree.class), Mockito.mock(PropertyState.class)));
        assertEquals("definesProperty", testConfig.ctx.method);
    }

    @Test
    public void testDefinesProperty2() {
        TestConfiguration testConfig = new TestConfiguration(false);
        addConfiguration(testConfig);

        assertFalse(compositeConfiguration.getContext().definesProperty(Mockito.mock(Tree.class), Mockito.mock(PropertyState.class)));
        assertEquals("definesProperty", testConfig.ctx.method);
    }

    @Test
    public void testDefinesContextRoot() {
        TestConfiguration testConfig = new TestConfiguration(true);
        addConfiguration(testConfig);

        assertTrue(compositeConfiguration.getContext().definesContextRoot(Mockito.mock(Tree.class)));
        assertEquals("definesContextRoot", testConfig.ctx.method);
    }

    @Test
    public void testDefinesContextRoot2() {
        TestConfiguration testConfig = new TestConfiguration(false);
        addConfiguration(testConfig);

        assertFalse(compositeConfiguration.getContext().definesContextRoot(Mockito.mock(Tree.class)));
        assertEquals("definesContextRoot", testConfig.ctx.method);
    }

    @Test
    public void testDefinesTree() {
        TestConfiguration testConfig = new TestConfiguration(true);
        addConfiguration(testConfig);

        assertTrue(compositeConfiguration.getContext().definesTree(Mockito.mock(Tree.class)));
        assertEquals("definesTree", testConfig.ctx.method);
    }

    @Test
    public void testDefinesTree2() {
        TestConfiguration testConfig = new TestConfiguration(false);
        addConfiguration(testConfig);

        assertFalse(compositeConfiguration.getContext().definesTree(Mockito.mock(Tree.class)));
        assertEquals("definesTree", testConfig.ctx.method);
    }

    @Test
    public void testDefinesLocation() {
        TestConfiguration testConfig = new TestConfiguration(true);
        addConfiguration(testConfig);

        assertTrue(compositeConfiguration.getContext().definesLocation(TreeLocation.create(Mockito.mock(Tree.class))));
        assertEquals("definesLocation", testConfig.ctx.method);
    }

    @Test
    public void testDefinesLocation2() {
        TestConfiguration testConfig = new TestConfiguration(false);
        addConfiguration(testConfig);

        assertFalse(compositeConfiguration.getContext().definesLocation(TreeLocation.create(Mockito.mock(Tree.class))));
        assertEquals("definesLocation", testConfig.ctx.method);
    }

    @Test
    public void testDefinesInternal() {
        TestConfiguration testConfig = new TestConfiguration(true);
        addConfiguration(testConfig);

        assertTrue(compositeConfiguration.getContext().definesInternal(Mockito.mock(Tree.class)));
        assertEquals("definesInternal", testConfig.ctx.method);
    }


    @Test
    public void testDefinesInternal2() {
        TestConfiguration testConfig = new TestConfiguration(false);
        addConfiguration(testConfig);

        assertFalse(compositeConfiguration.getContext().definesInternal(Mockito.mock(Tree.class)));
        assertEquals("definesInternal", testConfig.ctx.method);
    }


    private static final class TestConfiguration extends SecurityConfiguration.Default {

        private final TestContext ctx;

        private TestConfiguration() {
            this(false);
        }

        private TestConfiguration(boolean returnValue) {
            this.ctx = new TestContext(returnValue);
        }

        @Nonnull
        @Override
        public Context getContext() {
            return ctx;
        }
    }

    private static final class TestContext extends Context.Default {

        private String method;

        private final boolean returnValue;

        private TestContext(boolean returnValue) {
            this.returnValue = returnValue;
        }

        @Override
        public boolean definesProperty(@Nonnull Tree parent, @Nonnull PropertyState property) {
            method = "definesProperty";
            return returnValue;
        }

        @Override
        public boolean definesContextRoot(@Nonnull Tree tree) {
            method = "definesContextRoot";
            return returnValue;
        }

        @Override
        public boolean definesTree(@Nonnull Tree tree) {
            method = "definesTree";
            return returnValue;
        }

        @Override
        public boolean definesLocation(@Nonnull TreeLocation location) {
            method = "definesLocation";
            return returnValue;
        }

        @Override
        public boolean definesInternal(@Nonnull Tree tree) {
            method = "definesInternal";
            return returnValue;
        }
    }
}