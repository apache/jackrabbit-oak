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

/**
 * Unit cases for {@link Validate}
 */
public class ValidateTest {

    @Test(expected = IllegalArgumentException.class)
    public void checkArgumentFalse() {
        Validate.checkArgument(false);
    }

    @Test
    public void checkArgumentTrue() {
        Validate.checkArgument(true);
    }

    @Test
    public void checkArgumentFalseWithMessage() {
        try {
            Validate.checkArgument(false, "foo%");
            fail("exception expected");
        } catch (IllegalArgumentException ex) {
            assertEquals("foo%", ex.getMessage());
        }
    }

    @Test
    public void checkArgumentTrueWithMessage() {
        Validate.checkArgument(true, "foo%");
    }

    @Test
    public void checkArgumentFalseWithMessageTemplate() {
        try {
            Validate.checkArgument(false, "foo %s bar %s", "qux1", "qux2");
            fail("exception expected");
        } catch (IllegalArgumentException ex) {
            assertEquals("foo qux1 bar qux2", ex.getMessage());
        }
    }

    @Test
    public void checkArgumentFalseWithMessageTemplateNull() {
        try {
            Validate.checkArgument(false, "foo %s bar %s", null, "qux2");
            fail("exception expected");
        } catch (IllegalArgumentException ex) {
            assertEquals("foo null bar qux2", ex.getMessage());
        }
    }

    @Test
    public void checkArgumentFalseWithMessageTemplateTooFew() {
        try {
            Validate.checkArgument(false, "foo %s bar %s", "qux2");
            fail("exception expected");
        } catch (IllegalArgumentException ex) {
            // expected, thrown by String.format
        }
    }

    @Test
    public void checkArgumentFalseWithMessageTemplateTooMany() {
        try {
            Validate.checkArgument(false, "foo %s bar %s", 1, 2, 3);
            fail("exception expected");
        } catch (IllegalArgumentException ex) {
            assertEquals("foo 1 bar 2", ex.getMessage());
        }
    }

    // OAK-11209

    @Test(expected = IllegalStateException.class)
    public void checkStateFalse() {
        Validate.checkState(false);
    }

    @Test
    public void checkStateTrue() {
        Validate.checkState(true);
    }

    @Test
    public void checkStateFalseWithMessage() {
        try {
            Validate.checkState(false, "foo%");
            fail("exception expected");
        } catch (IllegalStateException ex) {
            assertEquals("foo%", ex.getMessage());
        }
    }

    @Test
    public void checkStateTrueWithMessage() {
        Validate.checkState(true, "foo%");
    }

    @Test
    public void checkStateFalseWithMessageTemplate() {
        try {
            Validate.checkState(false, "foo %s bar %s", "qux1", "qux2");
            fail("exception expected");
        } catch (IllegalStateException ex) {
            assertEquals("foo qux1 bar qux2", ex.getMessage());
        }
    }

    @Test
    public void checkStateFalseWithMessageTemplateNull() {
        try {
            Validate.checkState(false, "foo %s bar %s", null, "qux2");
            fail("exception expected");
        } catch (IllegalStateException ex) {
            assertEquals("foo null bar qux2", ex.getMessage());
        }
    }

    @Test
    public void checkStateFalseWithMessageTemplateTooFew() {
        try {
            Validate.checkState(false, "foo %s bar %s", "qux2");
            fail("exception expected");
        } catch (IllegalArgumentException ex) {
            // expected, thrown by String.format
        }
    }

    @Test
    public void checkStateFalseWithMessageTemplateTooMany() {
        try {
            Validate.checkState(false, "foo %s bar %s", 1, 2, 3);
            fail("exception expected");
        } catch (IllegalStateException ex) {
            assertEquals("foo 1 bar 2", ex.getMessage());
        }
    }

    // OAK-11209 END

    @Test
    public void countArguments() {
        assertEquals(3, Validate.countArguments("1%s2%d%%%c"));
    }

    @Test(expected = NullPointerException.class)
    public void checkTemplatemullParam() {
        Validate.checkTemplate("foo", (Object[]) null);
    }

    @Test
    public void checkTemplate() {
        assertFalse(Validate.checkTemplate("foo", "x"));
        assertTrue(Validate.checkTemplate("foo"));
    }
}