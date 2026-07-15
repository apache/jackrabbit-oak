/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.commit;

import static org.apache.jackrabbit.JcrConstants.JCR_CREATED;
import static org.apache.jackrabbit.JcrConstants.JCR_LASTMODIFIED;
import static org.apache.jackrabbit.oak.api.Type.DATE;
import static org.apache.jackrabbit.util.ISO8601.parse;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Calendar;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.util.ISO8601;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JcrLastModifiedConflictHandlerTest {

    private JcrLastModifiedConflictHandler handler;
    private NodeBuilder nb;

    @Before
    public void before() {
        handler = new JcrLastModifiedConflictHandler();
        nb = mock(NodeBuilder.class);
    }

    @Test
    public void testIgnoredDate() throws Exception {
        String propertyName = "ignored";
        PropertyState ours = mockProperty(propertyName, "invalidDateValue");
        PropertyState theirs = createDateProperty(propertyName);
        ThreeWayConflictHandler.Resolution resolution = handler.addExistingProperty(nb, ours, theirs);
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, resolution);
        resolution = handler.changeChangedProperty(nb, ours, theirs, theirs);
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, resolution);
        verifyNoInteractions(nb);
    }

    @Test
    public void testOursNotValidDate() throws Exception {
        testOursNotValidDate(JCR_CREATED);
        testOursNotValidDate(JCR_LASTMODIFIED);
    }

    private void testOursNotValidDate(@NotNull String propertyName) {
        PropertyState ours = mockProperty(propertyName, "invalidDateValue");
        PropertyState theirs = createDateProperty(propertyName);
        ThreeWayConflictHandler.Resolution resolution = handler.addExistingProperty(nb, ours, theirs);
        assertSame(ThreeWayConflictHandler.Resolution.MERGED, resolution);
        verify(nb).setProperty(propertyName, parse(theirs.getValue(Type.DATE)));
    }

    @Test
    public void testTheirsNotValidDate() throws Exception {
        testTheirsNotValidDate(JCR_CREATED);
        testTheirsNotValidDate(JCR_LASTMODIFIED);
    }

    private void testTheirsNotValidDate(@NotNull String propertyName) throws Exception {
        PropertyState ours = createDateProperty(propertyName);
        PropertyState theirs = mockProperty(propertyName, "invalidDateValue");
        ThreeWayConflictHandler.Resolution resolution = handler.addExistingProperty(nb, ours, theirs);
        assertSame(ThreeWayConflictHandler.Resolution.MERGED, resolution);
        verify(nb).setProperty(propertyName, parse(ours.getValue(Type.DATE)));
    }

    @Test
    public void testBothNotValidDate() throws Exception {
        testBothNotValidDate(JCR_CREATED);
        testBothNotValidDate(JCR_LASTMODIFIED);
    }

    private void testBothNotValidDate(@NotNull String propertyName) throws Exception {
        PropertyState ours = mockProperty(propertyName, "invalidDateValue");
        PropertyState theirs = mockProperty(propertyName, "invalidDateValue");
        ThreeWayConflictHandler.Resolution resolution = handler.addExistingProperty(nb, ours, theirs);
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, resolution);
        verifyNoInteractions(nb);
        resolution = handler.changeChangedProperty(nb, ours, theirs, createDateProperty(propertyName));
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, resolution);
        verifyNoInteractions(nb);
    }

    @Test
    public void testOursBeforeTheirsLastModified() throws Exception {
        PropertyState ours = createDateProperty(JCR_LASTMODIFIED);
        // enforce that the second is a bit later
        Thread.sleep(1);
        PropertyState theirs = createDateProperty(JCR_LASTMODIFIED);

        ThreeWayConflictHandler.Resolution resolution = handler.changeChangedProperty(nb, ours, theirs, createDateProperty(JCR_LASTMODIFIED));
        assertSame(ThreeWayConflictHandler.Resolution.MERGED, resolution);
        verify(nb).setProperty(JCR_LASTMODIFIED, parse(theirs.getValue(Type.DATE)));
    }

    @Test
    public void testOursBeforeTheirsCreated() throws Exception {
        PropertyState ours = createDateProperty(JCR_CREATED);
        // enforce that the second is a bit later
        Thread.sleep(1);
        PropertyState theirs = createDateProperty(JCR_CREATED);

        ThreeWayConflictHandler.Resolution resolution = handler.changeChangedProperty(nb, ours, theirs, createDateProperty(JCR_LASTMODIFIED));
        assertSame(ThreeWayConflictHandler.Resolution.MERGED, resolution);
        verify(nb).setProperty(JCR_CREATED, parse(ours.getValue(Type.DATE)));
    }

    @Test
    public void updates() throws Exception {

        ContentRepository repo = newRepo(handler);
        Root root = login(repo);
        setup(root);

        Root ourRoot = login(repo);
        Root theirRoot = login(repo);

        PropertyState p0 = createDateProperty(JCR_CREATED);
        ourRoot.getTree("/c").setProperty(p0);

        TimeUnit.MILLISECONDS.sleep(50);
        theirRoot.getTree("/c").setProperty(createDateProperty(JCR_CREATED));

        ourRoot.commit();
        theirRoot.commit();

        root.refresh();
        Assert.assertEquals(p0, root.getTree("/c").getProperty(JCR_CREATED));
        TimeUnit.MILLISECONDS.sleep(50);

        ourRoot.refresh();
        theirRoot.refresh();

        ourRoot.getTree("/c").setProperty(createDateProperty(JCR_LASTMODIFIED));
        TimeUnit.MILLISECONDS.sleep(50);
        PropertyState p1 = createDateProperty(JCR_LASTMODIFIED);
        theirRoot.getTree("/c").setProperty(p1);

        ourRoot.commit();
        theirRoot.commit();

        root.refresh();
        Assert.assertEquals(p1, root.getTree("/c").getProperty(JCR_LASTMODIFIED));
    }

    @NotNull
    public static PropertyState createDateProperty(@NotNull String name) {
        String now = ISO8601.format(Calendar.getInstance());
        return PropertyStates.createProperty(name, now, DATE);
    }

    @SuppressWarnings("unchecked")
    @NotNull
    private static PropertyState mockProperty(@NotNull String name, @NotNull String value) {
        PropertyState ps = when(mock(PropertyState.class).getName()).thenReturn(name).getMock();
        when(ps.getValue(any(Type.class))).thenReturn(value);
        return ps;
    }

    @NotNull
    private static ContentRepository newRepo(@NotNull ThreeWayConflictHandler handler) {
        return new Oak().with(new OpenSecurityProvider()).with(handler).createContentRepository();
    }

    @NotNull
    private static Root login(ContentRepository repo) throws Exception {
        return repo.login(null, null).getLatestRoot();
    }

    private static void setup(@NotNull Root root) throws Exception {
        Tree tree = root.getTree("/");
        tree.addChild("c");
        root.commit();
    }

}
