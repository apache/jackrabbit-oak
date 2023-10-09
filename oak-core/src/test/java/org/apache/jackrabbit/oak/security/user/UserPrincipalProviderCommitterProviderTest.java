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

package org.apache.jackrabbit.oak.security.user;

import junit.framework.TestCase;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.security.Principal;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UserPrincipalProviderCommitterProviderTest extends TestCase {

    static final int NUM_THREADS = 1000;

    public void testCacheGroups() throws Exception {
        Tree authorizableNode = mock(Tree.class);
        when(authorizableNode.getChild(CacheConstants.REP_CACHE)).thenReturn(authorizableNode);
        when(authorizableNode.exists()).thenReturn(true);
        when(authorizableNode.getPath()).thenReturn("/path/to/authorizableNode");

        Set<Principal> groupPrincipals = mock(Set.class);
        when(groupPrincipals.isEmpty()).thenReturn(true);
        MockRoot root = new MockRoot();

        PrincipalCacheCommitterThread[] thread = new PrincipalCacheCommitterThread[NUM_THREADS];
        CacheThreadProvider userPrincipalProviderCommitterProvider = CacheThreadProvider.getInstance();
        for (int i = 0; i < NUM_THREADS; i++) {
            thread[i] = CacheThreadProvider.getInstance().cacheGroups(authorizableNode, groupPrincipals, 1000, root);
        }
        for (int i = 0; i < NUM_THREADS; i++) {
            if (thread[i] != null) {
                thread[i].join();
            }
        }
        assertEquals(root.getCommitInvocations(), 1);
        assertEquals(0, userPrincipalProviderCommitterProvider.getCommitterThreadMap().size());

    }

    public void testCacheGroups2Nodes() throws Exception {
        Tree authorizableNode = mock(Tree.class);
        when(authorizableNode.getChild(CacheConstants.REP_CACHE)).thenReturn(authorizableNode);
        when(authorizableNode.exists()).thenReturn(true);
        when(authorizableNode.getPath()).thenReturn("/path/to/authorizableNode");

        Tree authorizableNode2 = mock(Tree.class);
        when(authorizableNode2.getChild(CacheConstants.REP_CACHE)).thenReturn(authorizableNode);
        when(authorizableNode2.exists()).thenReturn(true);
        when(authorizableNode2.getPath()).thenReturn("/path/to/authorizableNode2");


        Set<Principal> groupPrincipals = mock(Set.class);
        when(groupPrincipals.isEmpty()).thenReturn(true);
        MockRoot root = new MockRoot();

        PrincipalCacheCommitterThread[] thread = new PrincipalCacheCommitterThread[NUM_THREADS];
        CacheThreadProvider userPrincipalProviderCommitterProvider = CacheThreadProvider.getInstance();
        for (int i = 0; i < NUM_THREADS; i++) {
            thread[i] = CacheThreadProvider.getInstance().cacheGroups(authorizableNode, groupPrincipals, 1000, root);
            thread[i] = CacheThreadProvider.getInstance().cacheGroups(authorizableNode2, groupPrincipals, 1000, root);
        }
        for (int i = 0; i < NUM_THREADS; i++) {
            if (thread[i] != null) {
                thread[i].join();
            }
        }
        assertEquals(root.getCommitInvocations(), 2);
        assertEquals(0, userPrincipalProviderCommitterProvider.getCommitterThreadMap().size());

    }

    class MockRoot implements Root {

        int commitInvocations = 0;

        @Override
        public boolean move(String s, String s1) {
            return false;
        }

        @Override
        public @NotNull Tree getTree(@NotNull String s) {
            return null;
        }

        @Override
        public void rebase() {

        }

        @Override
        public void refresh() {

        }

        @Override
        public void commit(@NotNull Map<String, Object> map) throws CommitFailedException {
            commitInvocations++;
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        public int getCommitInvocations() {
            return commitInvocations;
        }

        @Override
        public void commit() throws CommitFailedException {

        }

        @Override
        public boolean hasPendingChanges() {
            return false;
        }

        @Override
        public @NotNull QueryEngine getQueryEngine() {
            return null;
        }

        @Override
        public @NotNull Blob createBlob(@NotNull InputStream inputStream) throws IOException {
            return null;
        }

        @Override
        public @Nullable Blob getBlob(@NotNull String s) {
            return null;
        }

        @Override
        public @NotNull ContentSession getContentSession() {
            return null;
        }
    }
}