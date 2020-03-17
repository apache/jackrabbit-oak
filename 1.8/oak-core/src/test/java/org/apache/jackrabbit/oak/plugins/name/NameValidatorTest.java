/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.name;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.junit.Test;

public class NameValidatorTest {

    private final Validator validator =
            new NameValidator(Collections.singleton("valid"));

    @Test(expected = CommitFailedException.class)
    public void testCurrentPath() throws CommitFailedException {
        validator.childNodeAdded(".", EMPTY_NODE);
    }

    @Test(expected = CommitFailedException.class)
    public void testParentPath() throws CommitFailedException {
        validator.childNodeAdded("..", EMPTY_NODE);
    }

    @Test // valid as of OAK-182
    public void testEmptyPrefix() throws CommitFailedException {
        validator.childNodeAdded(":name", EMPTY_NODE);
    }

    @Test(expected = CommitFailedException.class)
    public void testInvalidPrefix() throws CommitFailedException {
        validator.childNodeAdded("invalid:name", EMPTY_NODE);
    }

    @Test(expected = CommitFailedException.class)
    public void testTrailingWhitespace() throws CommitFailedException {
        validator.childNodeAdded("name ", EMPTY_NODE);
    }

    @Test(expected = CommitFailedException.class)
    public void testLeadingWhitespace() throws CommitFailedException {
        validator.childNodeAdded(" name", EMPTY_NODE);
    }

    @Test(expected = CommitFailedException.class)
    public void testOnlyWhitespace() throws CommitFailedException {
        validator.childNodeAdded(" ", EMPTY_NODE);
    }

    @Test
    public void testValidPrefix() throws CommitFailedException {
        validator.childNodeAdded("valid:name", EMPTY_NODE);
    }

    @Test(expected = CommitFailedException.class)
    public void testSlashName() throws CommitFailedException {
        validator.childNodeAdded("invalid/name", EMPTY_NODE);
    }

    @Test
    public void testValidIndexInName() throws CommitFailedException {
        validator.childNodeAdded("name[1]", EMPTY_NODE);
    }

    @Test(expected = CommitFailedException.class)
    public void testInvalidIndexInName() throws CommitFailedException {
        validator.childNodeAdded("name[x]", EMPTY_NODE);
    }

    @Test
    public void testValidName() throws CommitFailedException {
        validator.childNodeAdded("name", EMPTY_NODE);
    }

    @Test(expected = CommitFailedException.class)
    public void testNameWithLineBreaks() throws Exception{
        validator.childNodeAdded("name\tx", EMPTY_NODE);
    }

    @Test
    public void testDeleted() throws CommitFailedException {
        validator.childNodeDeleted(".", EMPTY_NODE);
        validator.childNodeDeleted("..", EMPTY_NODE);
        validator.childNodeDeleted("valid:name", EMPTY_NODE);
        validator.childNodeDeleted("invalid:name", EMPTY_NODE);
        validator.childNodeDeleted("invalid/name", EMPTY_NODE);
    }

    @Test
    public void testEscaping() {
        assertEquals("abc", NameValidator.getPrintableName("abc"));
        assertEquals("\\t\\r\\n\\b\\f", NameValidator.getPrintableName("\t\r\n\b\f"));
        assertEquals("\\u00e0", NameValidator.getPrintableName("\u00e0"));
    }
}
