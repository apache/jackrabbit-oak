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
package org.apache.jackrabbit.mongomk;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * <code>ConflictTest</code> checks
 * <a href="http://wiki.apache.org/jackrabbit/Conflict%20handling%20through%20rebasing%20branches">conflict handling</a>.
 */
public class ConflictTest extends BaseMongoMKTest {

    @Test
    public void addExistingProperty() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        mk.commit("/foo", "^\"prop\":\"value\"", rev, null);

        try {
            mk.commit("/foo", "^\"prop\":\"value\"", rev, null);
            fail("Must fail with conflict for addExistingProperty");
        } catch (MicroKernelException e) {
            // expected
        }
    }

    @Test
    public void removeRemovedProperty() {
        String rev = mk.commit("/", "+\"foo\":{\"prop\":\"value\"}", null, null);
        mk.commit("/foo", "^\"prop\":null", rev, null);

        try {
            mk.commit("/foo", "^\"prop\":null", rev, null);
            fail("Must fail with conflict for removeRemovedProperty");
        } catch (MicroKernelException e) {
            // expected
        }
    }

    @Test
    public void removeChangedProperty() {
        String rev = mk.commit("/", "+\"foo\":{\"prop\":\"value\"}", null, null);
        mk.commit("/foo", "^\"prop\":\"bar\"", rev, null);

        try {
            mk.commit("/foo", "^\"prop\":null", rev, null);
            fail("Must fail with conflict for removeChangedProperty");
        } catch (MicroKernelException e) {
            // expected
        }
    }

    @Test
    public void changeRemovedProperty() {
        String rev = mk.commit("/", "+\"foo\":{\"prop\":\"value\"}", null, null);
        mk.commit("/foo", "^\"prop\":null", rev, null);

        try {
            mk.commit("/foo", "^\"prop\":\"bar\"", rev, null);
            fail("Must fail with conflict for changeRemovedProperty");
        } catch (MicroKernelException e) {
            // expected
        }
    }

    @Test
    public void changeChangedProperty() {
        String rev = mk.commit("/", "+\"foo\":{\"prop\":\"value\"}", null, null);
        mk.commit("/foo", "^\"prop\":\"bar\"", rev, null);

        try {
            mk.commit("/foo", "^\"prop\":\"baz\"", rev, null);
            fail("Must fail with conflict for changeChangedProperty");
        } catch (MicroKernelException e) {
            // expected
        }
    }

    @Test
    public void addExistingNode() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        mk.commit("/foo", "+\"bar\":{}", rev, null);

        try {
            mk.commit("/foo", "+\"bar\":{}", rev, null);
            fail("Must fail with conflict for addExistingNode");
        } catch (MicroKernelException e) {
            // expected
        }
    }

    @Test
    public void removeRemovedNode() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        mk.commit("/", "-\"foo\"", rev, null);

        try {
            mk.commit("/", "-\"foo\"", rev, null);
            fail("Must fail with conflict for removeRemovedNode");
        } catch (MicroKernelException e) {
            // expected
        }
    }

    @Test
    @Ignore
    public void removeChangedNode() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        mk.commit("/foo", "^\"prop\":\"value\"", rev, null);

        try {
            mk.commit("/", "-\"foo\"", rev, null);
            fail("Must fail with conflict for removeChangedNode");
        } catch (MicroKernelException e) {
            // expected
        }
    }

    @Test
    public void changeRemovedNode() {
        String rev = mk.commit("/", "+\"foo\":{}", null, null);
        mk.commit("/", "-\"foo\"", rev, null);

        try {
            mk.commit("/foo", "^\"prop\":\"value\"", rev, null);
            fail("Must fail with conflict for changeRemovedNode");
        } catch (MicroKernelException e) {
            // expected
        }
    }
}
