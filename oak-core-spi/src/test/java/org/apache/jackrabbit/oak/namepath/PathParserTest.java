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
package org.apache.jackrabbit.oak.namepath;

import org.junit.Ignore;
import org.junit.Test;

import javax.jcr.RepositoryException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PathParserTest {

    private enum ParserCallbackResultType {

        CALLBACK_ROOT,
        CALLBACK_CURRENT,
        CALLBACK_PARENT,
        CALLBACK_NAME,
        CALLBACK_ERROR;
    }

    private static class ParserCallbackResult {

        private final ParserCallbackResultType type;
        private final String data;
        private final int index;

        ParserCallbackResult(ParserCallbackResultType type, String data, int index) {
            this.type = type;
            this.data = data;
            this.index = index;
        }

        ParserCallbackResultType getType() {
            return type;
        }

        String getData() {
            return data;
        }

        int getIndex() {
            return index;
        }

        @Override
        public int hashCode() {
            if (type == ParserCallbackResultType.CALLBACK_NAME) {
                return data == null? 0 : data.hashCode();
            }
            if (type == ParserCallbackResultType.CALLBACK_ERROR) {
                return data == null? 0 : data.hashCode();
            }
            return type.ordinal();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof ParserCallbackResult) {
                ParserCallbackResult result = (ParserCallbackResult) obj;
                if (result.getType() == getType()) {
                    return result.getData() == null || data == null || (result.getData().equals(data) && result.getIndex() == index);
                }
            }
            return false;
        }

        @Override
        public String toString() {
            return data == null ? "null" : data;
        }
    }

    private static final ParserCallbackResult CALLBACKRESULT_ROOT = new ParserCallbackResult(ParserCallbackResultType.CALLBACK_ROOT, null, 0);
    private static final ParserCallbackResult CALLBACKRESULT_CURRENT = new ParserCallbackResult(ParserCallbackResultType.CALLBACK_CURRENT, null, 0);
    private static final ParserCallbackResult CALLBACKRESULT_PARENT = new ParserCallbackResult(ParserCallbackResultType.CALLBACK_PARENT, null, 0);
    private static ParserCallbackResult CALLBACKRESULT_NAME(String name) {
        return new ParserCallbackResult(ParserCallbackResultType.CALLBACK_NAME, name, 0);
    }
    private static ParserCallbackResult CALLBACKRESULT_NAME(String name, int index) {
        return new ParserCallbackResult(ParserCallbackResultType.CALLBACK_NAME, name, index);
    }
    private static ParserCallbackResult CALLBACKRESULT_ERROR(String error) {
        return new ParserCallbackResult(ParserCallbackResultType.CALLBACK_ERROR, error, 0);
    }
    private static ParserCallbackResult CALLBACKRESULT_ERROR_ANY = new ParserCallbackResult(ParserCallbackResultType.CALLBACK_ERROR, null, 0);

    private static class TestListener implements JcrPathParser.Listener {

        private List<ParserCallbackResult> expectedCallbackResults;
        private List<ParserCallbackResult> callbackResults = new ArrayList<>();

        TestListener(ParserCallbackResult ... expectedCallbackResult) {
            this.expectedCallbackResults = Arrays.stream(expectedCallbackResult).collect(Collectors.toList());
        }

        @Override
        public boolean name(String name, int index) {
            callbackResults.add(CALLBACKRESULT_NAME(name, index));
            return true;
        }

        @Override
        public boolean root() {
            callbackResults.add(CALLBACKRESULT_ROOT);
            return true;
        }

        @Override
        public boolean current() {
            callbackResults.add(CALLBACKRESULT_CURRENT);
            return true;
        }

        @Override
        public boolean parent() {
            callbackResults.add(CALLBACKRESULT_PARENT);
            return true;
        }

        @Override
        public void error(String message) {
            callbackResults.add(CALLBACKRESULT_ERROR(message));
        }

        public void evaluate() {
            assertArrayEquals("Parser produced an unexpected sequence of callbacks", expectedCallbackResults.toArray(), callbackResults.toArray());
        }

        public void reset() {
            callbackResults.clear();
        }
    }

    @Test
    @Ignore //OAK-10621
    public void testGeneralPath() {
        String path = "/a/{http://www.jcp.org/jcr/1.0}b/{http://www.jcp.org/jcr/1.0}c[50]/{d}e/./../x:y[1]/z";
        TestListener listener = new TestListener(
                CALLBACKRESULT_ROOT,
                CALLBACKRESULT_NAME("a"),
                CALLBACKRESULT_NAME("{http://www.jcp.org/jcr/1.0}b"),
                CALLBACKRESULT_NAME("{http://www.jcp.org/jcr/1.0}c", 50),
                CALLBACKRESULT_NAME("{d}e"),
                CALLBACKRESULT_CURRENT,
                CALLBACKRESULT_PARENT,
                CALLBACKRESULT_NAME("x:y", 1),
                CALLBACKRESULT_NAME("z")
        );
        verifyResult(path, listener, false);

        path += ']';
        listener = new TestListener(
                CALLBACKRESULT_ROOT,
                CALLBACKRESULT_NAME("a"),
                CALLBACKRESULT_NAME("{http://www.jcp.org/jcr/1.0}b"),
                CALLBACKRESULT_NAME("{http://www.jcp.org/jcr/1.0}c", 50),
                CALLBACKRESULT_NAME("{d}e"),
                CALLBACKRESULT_CURRENT,
                CALLBACKRESULT_PARENT,
                CALLBACKRESULT_NAME("x:y", 1),
                CALLBACKRESULT_ERROR(errorCharacterNotAllowedInName(path, ']'))
        );
        verifyResult(path, listener, false);
    }

    @Test
    public void testEmptyPath() {
        boolean result;

        String path = null;
        TestListener listener = new TestListener();
        verifyResult(path, listener, true);

        path = "";
        verifyResult(path, listener, true);
    }

    @Test
    @Ignore //OAK-10621
    public void testExpandendName() {
        boolean result;

        final String prefix = "{http://www.jcp.org/jcr/1.0}";
        String path = prefix;
        TestListener listener = new TestListener(
                CALLBACKRESULT_ERROR(errorEmptyLocalName(path))
        );
        verifyResult(path, listener, false);

        path = prefix + "a";
        listener = new TestListener(
                CALLBACKRESULT_NAME(prefix + "a")
        );
        verifyResult(path, listener, true);

        path = "/" + prefix + "a";
        listener = new TestListener(
                CALLBACKRESULT_ROOT,
                CALLBACKRESULT_NAME(prefix + "a")
        );
        verifyResult(path, listener, true);

        path = prefix + "/b";
        listener = new TestListener(
                CALLBACKRESULT_ERROR(errorEmptyLocalName(path))
        );
        verifyResult(path, listener, false);

        path = "{a}b";
        listener = new TestListener(
                CALLBACKRESULT_NAME("{a}b")
        );
        verifyResult(path, listener, true);

        path = "/{a}b";
        listener = new TestListener(
                CALLBACKRESULT_ROOT,
                CALLBACKRESULT_NAME("{a}b")
        );
        verifyResult(path, listener, true);

        path = "{a}b[1]";
        listener = new TestListener(
                CALLBACKRESULT_NAME("{a}b", 1)
        );
        verifyResult(path, listener, true);
    }

    //TODO add more tests to cover all edge cases

    @Test
    public void testUnexpectedOpeningSquareBracket() throws RepositoryException {
        String path = "[";
        TestListener listener = new TestListener(
                CALLBACKRESULT_ERROR(errorCharacterNotAllowedInName(path,'['))
        );
        verifyResult(path, listener, false);

        path = "/[";
        listener = new TestListener(
                CALLBACKRESULT_ROOT,
                CALLBACKRESULT_ERROR(errorCharacterNotAllowedInName(path,'['))
        );
        verifyResult(path, listener, false);

        path = "./[";
        listener = new TestListener(
                CALLBACKRESULT_CURRENT,
                CALLBACKRESULT_ERROR(errorCharacterNotAllowedInName(path,'['))
        );
        verifyResult(path, listener, false);

        path = "../[";
        listener = new TestListener(
                CALLBACKRESULT_PARENT,
                CALLBACKRESULT_ERROR(errorCharacterNotAllowedInName(path,'['))
        );
        verifyResult(path, listener, false);

        path = ".[";
        listener = new TestListener(
                CALLBACKRESULT_ERROR(errorCharacterNotAllowedInName(path,'['))
        );
        verifyResult(path, listener, false);

        path = "..[";
        listener = new TestListener(
                CALLBACKRESULT_ERROR(errorCharacterNotAllowedInName(path,'['))
        );
        verifyResult(path, listener, false);

        path = "{[}";
        listener = new TestListener(
                CALLBACKRESULT_ERROR(errorCharacterNotAllowedInName(path,'['))
        );
        verifyResult(path, listener, false);

        path = "a[[";
        listener = new TestListener(
                CALLBACKRESULT_ERROR_ANY
                //the parser actually produces an error, but we should change the error message to something like this
                //CALLBACKRESULT_ERROR(errorClosingQuareBracketExpected(path))
        );
        verifyResult(path, listener, false);
    }

    @Test
    public void testMissingClosingSquareBracket() throws RepositoryException {
        String path = "/a[";
        TestListener listener = new TestListener(
                CALLBACKRESULT_ROOT,
                CALLBACKRESULT_ERROR_ANY
                //the parser actually produces an error, but we should change the error message to something like this
                //CALLBACKRESULT_ERROR(errorClosingQuareBracketExpected(path))
        );
        verifyResult(path, listener, false);
    }

    @Test
    public void testUnxepectedClosingSquareBracket() throws RepositoryException {
        String path = "]";
        TestListener listener = new TestListener(
                CALLBACKRESULT_ERROR(errorCharacterNotAllowedInName(path, ']'))
        );
        verifyResult(path, listener, false);

        path = "/]";
        listener = new TestListener(
                CALLBACKRESULT_ROOT,
                CALLBACKRESULT_ERROR(errorCharacterNotAllowedInName(path, ']'))
        );
        verifyResult(path, listener, false);

        path = ".]";
        listener = new TestListener(
                //TODO improve error message?
                CALLBACKRESULT_ERROR(errorCharacterNotAllowedInName(path, ']'))
        );
        verifyResult(path, listener, false);

        path = "..]";
        listener = new TestListener(
                //TODO improve error message?
                CALLBACKRESULT_ERROR(errorCharacterNotAllowedInName(path, ']'))
        );
        verifyResult(path, listener, false);

        path = "{]}";
        listener = new TestListener(
                CALLBACKRESULT_ERROR(errorCharacterNotAllowedInName(path, ']'))
        );
        verifyResult(path, listener, false);

        path = "a[]]";
        listener = new TestListener(
                CALLBACKRESULT_ERROR(errorNumberFormatExceptionInIndex(path))
        );
        verifyResult(path, listener, false);
    }

    @Test
    @Ignore //OAK-10624
    public void testCurlyBracketsInNames() throws RepositoryException {
        String path = "/{";
        TestListener listener = new TestListener(
                CALLBACKRESULT_ROOT,
                CALLBACKRESULT_ERROR("'/{' is not a valid path. Missing '}'.")
        );
        verifyResult(path, listener, false);

        path = "/a{";
        listener = new TestListener(
                CALLBACKRESULT_ROOT,
                CALLBACKRESULT_NAME("a{")
        );
        verifyResult(path, listener, true);

        path = "/}";
        listener = new TestListener(
                CALLBACKRESULT_ROOT,
                CALLBACKRESULT_NAME("}")
        );
        verifyResult(path, listener, true);

        path = "/a}";
        listener = new TestListener(
                CALLBACKRESULT_ROOT,
                CALLBACKRESULT_NAME("a}")
        );
        verifyResult(path, listener, true);

        path = "/a}[1]";
        listener = new TestListener(
                CALLBACKRESULT_ROOT,
                CALLBACKRESULT_NAME("a}", 1)
        );
        verifyResult(path, listener, true);

        path = "/a{[1]";
        listener = new TestListener(
                CALLBACKRESULT_ROOT,
                CALLBACKRESULT_NAME("a{", 1)
        );
        verifyResult(path, listener, true);

        path = "/a{}";
        listener = new TestListener(
                CALLBACKRESULT_ROOT,
                CALLBACKRESULT_NAME("a{}")
        );
        verifyResult(path, listener, true);

        path = "/a{}[1]";
        listener = new TestListener(
                CALLBACKRESULT_ROOT,
                CALLBACKRESULT_NAME("a{}", 1)
        );
        verifyResult(path, listener, true);

        path = "/a}{";
        listener = new TestListener(
                CALLBACKRESULT_ROOT,
                CALLBACKRESULT_NAME("a}{")
        );
        verifyResult(path, listener, true);

        path = "/a}{[1]";
        listener = new TestListener(
                CALLBACKRESULT_ROOT,
                CALLBACKRESULT_NAME("a}{", 1)
        );
        verifyResult(path, listener, true);

        path = "/a{b}:c";
        listener = new TestListener(
                CALLBACKRESULT_ROOT,
                CALLBACKRESULT_ERROR("'/a{b}:c' is not a valid path. Invalid name prefix: a{b}")
        );
        verifyResult(path, listener, false);

        path = "/a{b:c";
        listener = new TestListener(
                CALLBACKRESULT_ROOT,
                CALLBACKRESULT_ERROR("'/a{b:c' is not a valid path. Invalid name prefix: a{b")
        );
        verifyResult(path, listener, false);

        path = "/ab}:c";
        listener = new TestListener(
                CALLBACKRESULT_ROOT,
                CALLBACKRESULT_ERROR("'/ab}:c' is not a valid path. Invalid name prefix: ab}")
        );
        verifyResult(path, listener, false);
    }

    @Test
    public void testMissingClosingCurlyBracket() throws RepositoryException {
        String path = "{a";
        TestListener listener = new TestListener(
                CALLBACKRESULT_ERROR(errorMissingClosingCurlyBracket(path))
        );
        verifyResult(path, listener, false);
    }

    @Test
    public void testPrefixes() throws RepositoryException {
        String path = "/a:b";
        TestListener listener = new TestListener(
                CALLBACKRESULT_ROOT,
                CALLBACKRESULT_NAME("a:b")
        );
        verifyResult(path, listener, true);

        path = "/a:b[1]";
        listener = new TestListener(
                CALLBACKRESULT_ROOT,
                CALLBACKRESULT_NAME("a:b", 1)
        );
        verifyResult(path, listener, true);

        // TODO fix error message (OAK-10625)
        path = "/a:";
        listener = new TestListener(
                CALLBACKRESULT_ROOT,
                CALLBACKRESULT_ERROR_ANY
        );
        verifyResult(path, listener, false);

        path = "/a:b:c";
        listener = new TestListener(
                CALLBACKRESULT_ROOT,
                CALLBACKRESULT_ERROR(errorCharacterNotAllowedInName(path, ':'))
        );
        verifyResult(path, listener, false);

        path = "/a:]";
        listener = new TestListener(
                CALLBACKRESULT_ROOT,
                CALLBACKRESULT_ERROR(errorCharacterNotAllowedInName(path, ']'))
        );
        verifyResult(path, listener, false);

        //TODO fix error message
        path = "/a:[1]";
        listener = new TestListener(
                CALLBACKRESULT_ROOT,
               CALLBACKRESULT_ERROR_ANY
               //CALLBACKRESULT_ERROR("'/a:[1]' is not a valid path. Local name after ':' expected")
        );
        verifyResult(path, listener, false);
    }

    private static String errorCharacterNotAllowedInName(String path, char c) {
        return "'" + path + "' is not a valid path. '" + c + "' not allowed in name.";
    }

    private static String errorClosingQuareBracketExpected(String path) {
        return "'" + path + "' is not a valid path. ']' expected after index.";
    }

    private static String errorNumberFormatExceptionInIndex(String path) {
        return "'" + path + "' is not a valid path. NumberFormatException in index: ";
    }

    private static String errorMissingClosingCurlyBracket(String path) {
        return "'" + path + "' is not a valid path. Missing '}'.";
    }

    private static String errorEmptyLocalName(String path) {
        return "'" + path + "' is not a valid path. Local name must not be empty.";
    }

    private static void verifyResult(String path, TestListener listener, boolean expectedResult) {
        listener.reset();
        boolean result = JcrPathParser.parse(path, listener);
        listener.evaluate();
        assertEquals(expectedResult, result);
        assertEquals(result, JcrPathParser.validate(path));
    }
}
