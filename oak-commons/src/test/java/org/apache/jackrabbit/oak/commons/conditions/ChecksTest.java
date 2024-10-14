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
package org.apache.jackrabbit.oak.commons.conditions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

public class ChecksTest {

    @Test(expected = IllegalArgumentException.class)
    public void checkArgumentFalse() {
        Checks.checkArgument(false);
    }

    @Test
    public void checkArgumentTrue() {
        Checks.checkArgument(true);
    }

    @Test
    public void checkArgumentFalseWithMessage() {
        try {
            Checks.checkArgument(false, "foo%");
            fail("exception expected");
        } catch (IllegalArgumentException ex) {
            assertEquals("foo%", ex.getMessage());
        }
    }

    @Test
    public void checkArgumentTrueWithMessage() {
        Checks.checkArgument(true, "foo%");
    }

    @Test
    public void checkArgumentFalseWithMessageTemplate() {
        try {
            Checks.checkArgument(false, "foo %s bar %s", "qux1", "qux2");
            fail("exception expected");
        } catch (IllegalArgumentException ex) {
            assertEquals("foo qux1 bar qux2", ex.getMessage());
        }
    }

    @Test
    public void checkArgumentFalseWithMessageTemplateNull() {
        try {
            Checks.checkArgument(false, "foo %s bar %s", null, "qux2");
            fail("exception expected");
        } catch (IllegalArgumentException ex) {
            assertEquals("foo null bar qux2", ex.getMessage());
        }
    }

    @Test
    public void checkArgumentFalseWithMessageTemplateTooFew() {
        try {
            Checks.checkArgument(false, "foo %s bar %s", "qux2");
            fail("exception expected");
        } catch (IllegalArgumentException ex) {
            // expected, thrown by String.format
        }
    }

    @Test
    public void checkArgumentFalseWithMessageTemplateTooMany() {
        try {
            Checks.checkArgument(false, "foo %s bar %s", 1, 2, 3);
            fail("exception expected");
        } catch (IllegalArgumentException ex) {
            assertEquals("foo 1 bar 2", ex.getMessage());
        }
    }

    @Test
    public void countArguments() {
        assertEquals(3, Checks.countArguments("1%s2%d%%%c"));
    }

    @Test(expected = NullPointerException.class)
    public void checkTemplatemullParam() {
        Checks.checkTemplate("foo", (Object[]) null);
    }

    @Test
    public void checkTemplate() {
        assertFalse(Checks.checkTemplate("foo", "x"));
        assertTrue(Checks.checkTemplate("foo"));
    }
}