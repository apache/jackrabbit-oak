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
package org.apache.jackrabbit.oak.plugins.index.old.mk;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test moving nodes.
 */
@RunWith(Parameterized.class)
public class ConflictingMoveTest extends MultiMkTestBase {

    public ConflictingMoveTest(String url) {
        super(url);
    }

    @Test
    public void collidingMove() {
        String head = mk.getHeadRevision();

        head = mk.commit("/", "+\"x\" : {} \n+\"y\" : {}\n", head, "");

        try {
            mk.commit("/", ">\"x\" : \"y\"", head, "");
            fail("expected to fail since /y already exists");
        } catch (MicroKernelException e) {
            // expected
        }
    }

    @Test
    public void conflictingMove() {
        String head = mk.getHeadRevision();

        head = mk.commit("/", "+\"a\" : {} \n+\"b\" : {}\n", head, "");

        String r1 = mk.commit("/", ">\"a\" : \"b/a\"", head, "");
        assertFalse(mk.nodeExists("/a", r1));
        assertTrue(mk.nodeExists("/b", r1));
        assertTrue(mk.nodeExists("/b/a", r1));

        try {
            mk.commit("/", ">\"b\" : \"a/b\"", head, "");
            fail();
        } catch (MicroKernelException e) {
            // expected to fail since /a doesn't exist anymore
        }
    }

    @Test
    public void conflictingAddDelete() {
        String head = mk.getHeadRevision();

        head = mk.commit("/", "+\"a\" : {} \n+\"b\" : {}\n", head, "");

        String r1 = mk.commit("/", "-\"b\" \n +\"a/x\" : {}", head, "");
        assertFalse(mk.nodeExists("/b", r1));
        assertTrue(mk.nodeExists("/a", r1));
        assertTrue(mk.nodeExists("/a/x", r1));

        try {
            mk.commit("/", "-\"a\" \n +\"b/x\" : {}", head, "");
            fail();
        } catch (MicroKernelException e) {
            // expected to fail since /b doesn't exist anymore
        }
    }

}
