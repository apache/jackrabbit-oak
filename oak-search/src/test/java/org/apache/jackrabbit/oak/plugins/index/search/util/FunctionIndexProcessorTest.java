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
package org.apache.jackrabbit.oak.plugins.index.search.util;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.junit.Test;

public class FunctionIndexProcessorTest {

    @Test
    public void getProperties() {
        assertEquals(
                "[a, test/b, test/:name]",
                Arrays.toString(
                FunctionIndexProcessor.getProperties(new String[] { "function",
                        "multiply", "@a", "add", "@test/b", "@test/:name" })));
    }

    @Test
    public void tryCalculateValue() {
        // length of a string
        assertEquals("value = 11",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("data", "Hello World").getNodeState(),
                new String[]{"function", "length", "@data"}).toString());
        // length of a binary property
        assertEquals("value = 100",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("data",
                        new ArrayBasedBlob(new byte[100]), Type.BINARY).getNodeState(),
                new String[]{"function", "length", "@data"}).toString());
        // uppercase
        assertEquals("value = HELLO WORLD",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("data", "Hello World").getNodeState(),
                new String[]{"function", "upper", "@data"}).toString());
        // lowercase
        assertEquals("value = hello world",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("data", "Hello World").getNodeState(),
                new String[]{"function", "lower", "@data"}).toString());
        // coalesce
        assertEquals("value = Hello",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().
                    setProperty("data1", "Hello").
                    setProperty("data2", "World").getNodeState(),
                new String[]{"function", "coalesce", "@data1", "@data2"}).toString());
        assertEquals("value = World",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("data2", "World").getNodeState(),
                new String[]{"function", "coalesce", "@data1", "@data2"}).toString());
        assertEquals("value = Hello",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("data1", "Hello").getNodeState(),
                new String[]{"function", "coalesce", "@data1", "@data2"}).toString());
        assertEquals("null",
                "" + FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("data3", "Hello").getNodeState(),
                new String[]{"function", "coalesce", "@data1", "@data2"}));
        // first
        assertEquals("value = Hello",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("array", Arrays.asList("Hello", "World"), Type.STRINGS).getNodeState(),
                new String[]{"function", "first", "@array"}).toString());
        assertEquals("value = Hello",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("array", Arrays.asList("Hello"), Type.STRINGS).getNodeState(),
                new String[]{"function", "first", "@array"}).toString());
        // name
        assertEquals("value = abc:content",
                FunctionIndexProcessor.tryCalculateValue("abc:content",
                EMPTY_NODE.builder().getNodeState(),
                new String[]{"function", "@:name"}).toString());
        // localname
        assertEquals("value = content",
                FunctionIndexProcessor.tryCalculateValue("abc:content",
                EMPTY_NODE.builder().getNodeState(),
                new String[]{"function", "@:localname"}).toString());
        // path
        assertEquals("value = /content",
                FunctionIndexProcessor.tryCalculateValue("/content",
                EMPTY_NODE.builder().getNodeState(),
                new String[]{"function", "@:path"}).toString());
    }

    @Test
    public void xpath() {
        checkConvert(
                "fn:upper-case(@data)",
                "function*upper*@data");
        checkConvert(
                "fn:lower-case(test/@data)",
                "function*lower*@test/data");
        checkConvert(
                "fn:lower-case(fn:name())",
                "function*lower*@:name");
        checkConvert(
                "fn:lower-case(fn:local-name())",
                "function*lower*@:localname");
        checkConvert(
                "fn:string-length(test/@data)",
                "function*length*@test/data");
        checkConvert(
                "fn:string-length(fn:name())",
                "function*length*@:name");
        checkConvert(
                "fn:path()",
                "function*@:path");
        checkConvert(
                "fn:string-length(fn:path())",
                "function*length*@:path");
        checkConvert(
                "fn:string-length(@jcr:path)",
                "function*length*@:path");
        checkConvert(
                "fn:lower-case(fn:upper-case(test/@data))",
                "function*lower*upper*@test/data");
        checkConvert("fn:coalesce(jcr:content/@foo2, jcr:content/@foo)",
                "function*coalesce*@jcr:content/foo2*@jcr:content/foo");
        checkConvert("fn:coalesce(jcr:content/@foo2,fn:lower-case(jcr:content/@foo))",
                "function*coalesce*@jcr:content/foo2*lower*@jcr:content/foo");
        checkConvert("fn:coalesce(jcr:content/@foo2,fn:coalesce(jcr:content/@foo, fn:lower-case(fn:name())))",
                "function*coalesce*@jcr:content/foo2*coalesce*@jcr:content/foo*lower*@:name");
        checkConvert("fn:coalesce(fn:coalesce(jcr:content/@foo2,jcr:content/@foo), fn:coalesce(@a:b, @c:d))",
                "function*coalesce*coalesce*@jcr:content/foo2*@jcr:content/foo*coalesce*@a:b*@c:d");
        checkConvert("jcr:first(jcr:content/@foo2)",
                "function*first*@jcr:content/foo2");
        checkConvert("jcr:if(jcr:exists(@alias), fn:path(), null)",
                "function*if*exists*@alias*@:path*null");
        checkConvert("jcr:if(jcr:exists(@alias), fn:path(), jcr:null())",
                "function*if*exists*@alias*@:path*null");
        checkConvert("jcr:exists(jcr:content/@foo2)",
                "function*exists*@jcr:content/foo2");
        checkConvert("jcr:op(@a, '+', @b)",
                "function*op*@a*'+'*@b");
    }

    @Test
    public void sql2() {
        checkConvert(
                "upper([data])",
                "function*upper*@data");
        checkConvert(
                "lower([test/data])",
                "function*lower*@test/data");
        checkConvert(
                "lower(name())",
                "function*lower*@:name");
        checkConvert(
                "lower(localname())",
                "function*lower*@:localname");
        checkConvert(
                "length([test/data])",
                "function*length*@test/data");
        checkConvert(
                "length(name())",
                "function*length*@:name");
        checkConvert(
                "path()",
                "function*@:path");
        checkConvert(
                "length(path())",
                "function*length*@:path");
        checkConvert(
                "length([jcr:path])",
                "function*length*@:path");
        checkConvert(
                "lower(upper([test/data]))",
                "function*lower*upper*@test/data");
        // the ']' character is escaped as ']]'
        checkConvert(
                "[strange[0]]]",
                "function*@strange[0]");
        checkConvert("coalesce([jcr:content/foo2],[jcr:content/foo])",
                "function*coalesce*@jcr:content/foo2*@jcr:content/foo");
        checkConvert("coalesce([jcr:content/foo2], lower([jcr:content/foo]))",
                "function*coalesce*@jcr:content/foo2*lower*@jcr:content/foo");
        checkConvert("coalesce([jcr:content/foo2] , coalesce([jcr:content/foo],lower(name())))",
                "function*coalesce*@jcr:content/foo2*coalesce*@jcr:content/foo*lower*@:name");
        checkConvert("coalesce(coalesce([jcr:content/foo2],[jcr:content/foo]), coalesce([a:b], [c:d]))",
                "function*coalesce*coalesce*@jcr:content/foo2*@jcr:content/foo*coalesce*@a:b*@c:d");
        checkConvert("first([jcr:content/foo2])",
                "function*first*@jcr:content/foo2");
        checkConvert("if(exists([alias]), path(), null)",
                "function*if*exists*@alias*@:path*null");
        checkConvert("exists([alias])",
                "function*exists*@alias");
        checkConvert("op([a], '+', [b])",
                "function*op*@a*'+'*@b");
        // the '*' operator must not be confused with the token separator
        checkConvert("op([a], '*', [b])",
                "function*op*@a*'*'*@b");
        checkConvert("op([a], 'is not', [b])",
                "function*op*@a*'is not'*@b");
    }

    @Test
    public void ifFunction() {
        // "if" returns the chosen operand's PropertyState as-is (keeping its
        // original name/type), unlike lower/upper/coalesce which rename to "value"
        // condition missing -> falseValue
        assertEquals("f = false-value",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("t", "true-value").setProperty("f", "false-value").getNodeState(),
                new String[]{"function", "if", "@cond", "@t", "@f"}).toString());
        // condition = 0 -> falseValue
        assertEquals("f = false-value",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("cond", 0L).
                    setProperty("t", "true-value").setProperty("f", "false-value").getNodeState(),
                new String[]{"function", "if", "@cond", "@t", "@f"}).toString());
        // condition = false -> falseValue
        assertEquals("f = false-value",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("cond", false).
                    setProperty("t", "true-value").setProperty("f", "false-value").getNodeState(),
                new String[]{"function", "if", "@cond", "@t", "@f"}).toString());
        // condition = true -> trueValue
        assertEquals("t = true-value",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("cond", true).
                    setProperty("t", "true-value").setProperty("f", "false-value").getNodeState(),
                new String[]{"function", "if", "@cond", "@t", "@f"}).toString());
        // condition = non-zero number -> trueValue
        assertEquals("t = true-value",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("cond", 1L).
                    setProperty("t", "true-value").setProperty("f", "false-value").getNodeState(),
                new String[]{"function", "if", "@cond", "@t", "@f"}).toString());
        // condition = non-empty string -> trueValue
        assertEquals("t = true-value",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("cond", "anything").
                    setProperty("t", "true-value").setProperty("f", "false-value").getNodeState(),
                new String[]{"function", "if", "@cond", "@t", "@f"}).toString());
        // falseValue itself missing -> null
        assertNull(FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("t", "true-value").getNodeState(),
                new String[]{"function", "if", "@cond", "@t", "@f"}));
        // sparse index scenario: if(exists([alias]), path(), null)
        assertEquals("value = /content",
                FunctionIndexProcessor.tryCalculateValue("/content",
                EMPTY_NODE.builder().setProperty("alias", "a").getNodeState(),
                new String[]{"function", "if", "exists", "@alias", "@:path", "null"}).toString());
        assertNull(FunctionIndexProcessor.tryCalculateValue("/content",
                EMPTY_NODE.builder().getNodeState(),
                new String[]{"function", "if", "exists", "@alias", "@:path", "null"}));
    }

    @Test
    public void existsFunction() {
        assertEquals("value = true",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("data", "Hello").getNodeState(),
                new String[]{"function", "exists", "@data"}).toString());
        assertEquals("value = false",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().getNodeState(),
                new String[]{"function", "exists", "@data"}).toString());
    }

    @Test
    public void coalesceWithStringLiteral() {
        // literal operands are not specific to op() -- they are supported
        // wherever a property reference or nested function is, e.g. as a
        // fallback default value for coalesce()
        checkConvert("coalesce([foo], 'default')",
                "function*coalesce*@foo*'default'");
        checkConvert("coalesce('default', [foo])",
                "function*coalesce*'default'*@foo");

        // foo present -> foo wins
        assertEquals("value = Hello",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("foo", "Hello").getNodeState(),
                new String[]{"function", "coalesce", "@foo", "'default'"}).toString());
        // foo missing -> literal fallback is used
        assertEquals("value = default",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().getNodeState(),
                new String[]{"function", "coalesce", "@foo", "'default'"}).toString());
    }

    @Test
    public void literalWithEscapedQuote() {
        // a single quote within a string literal is escaped as two single
        // quotes (''), the same convention used by SQL2Parser; full
        // round-trip: parse -> tokenize -> evaluate
        String polish = FunctionIndexProcessor.convertToPolishNotation("coalesce([foo], 'it''s a test')");
        assertEquals("function*coalesce*@foo*'it''s a test'", polish);

        String[] code = FunctionIndexProcessor.getFunctionCode(polish);
        assertEquals("[function, coalesce, @foo, 'it''s a test']", Arrays.toString(code));

        assertEquals("value = it's a test",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().getNodeState(),
                code).toString());
    }

    @Test
    public void opMultiplyLiteralRoundTrip() {
        // full round-trip: parse -> tokenize -> evaluate, for the '*' operator
        // literal specifically, since '*' is also the Polish-notation token
        // separator
        String polish = FunctionIndexProcessor.convertToPolishNotation("op([a], '*', [b])");
        String[] code = FunctionIndexProcessor.getFunctionCode(polish);
        assertEquals("[function, op, @a, '*', @b]", Arrays.toString(code));
        assertEquals("value = 6.0",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("a", 2L).setProperty("b", 3L).getNodeState(),
                code).toString());
    }

    @Test
    public void opFunction() {
        // math
        assertEquals("value = 3.0",
                calcOp("a", 1L, "b", 2L, "+").toString());
        assertEquals("value = -1.0",
                calcOp("a", 1L, "b", 2L, "-").toString());
        assertEquals("value = 6.0",
                calcOp("a", 2L, "b", 3L, "*").toString());
        assertEquals("value = 2.0",
                calcOp("a", 4L, "b", 2L, "/").toString());
        // math with a missing operand -> null
        assertNull(FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("b", 2L).getNodeState(),
                new String[]{"function", "op", "@a", "'+'", "@b"}));
        // math with a non-numeric operand -> null
        assertNull(FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("a", "not-a-number").setProperty("b", 2L).getNodeState(),
                new String[]{"function", "op", "@a", "'+'", "@b"}));

        // comparisons
        assertEquals("value = true", calcOp("a", 1L, "b", 2L, "<").toString());
        assertEquals("value = false", calcOp("a", 2L, "b", 2L, "<").toString());
        assertEquals("value = true", calcOp("a", 2L, "b", 2L, "<=").toString());
        assertEquals("value = true", calcOp("a", 2L, "b", 1L, ">").toString());
        assertEquals("value = true", calcOp("a", 2L, "b", 2L, ">=").toString());
        assertEquals("value = true", calcOp("a", 2L, "b", 2L, "=").toString());
        assertEquals("value = false", calcOp("a", 2L, "b", 3L, "=").toString());
        assertEquals("value = true", calcOp("a", 2L, "b", 3L, "<>").toString());
        // comparisons with a missing operand -> null
        assertNull(FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("b", 2L).getNodeState(),
                new String[]{"function", "op", "@a", "'='", "@b"}));

        // is / is not: null is comparable
        assertEquals("value = true",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().getNodeState(),
                new String[]{"function", "op", "@a", "'is'", "@b"}).toString());
        assertEquals("value = false",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("b", 2L).getNodeState(),
                new String[]{"function", "op", "@a", "'is'", "@b"}).toString());
        assertEquals("value = true",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("b", 2L).getNodeState(),
                new String[]{"function", "op", "@a", "'is not'", "@b"}).toString());

        // and / or, three-valued logic
        assertEquals("value = true", calcOp3("a", true, "b", true, "and").toString());
        assertEquals("value = false", calcOp3("a", true, "b", false, "and").toString());
        assertEquals("value = true", calcOp3("a", false, "b", true, "or").toString());
        assertEquals("value = true", calcOp3("a", true, "b", false, "or").toString());
        // false and null -> false; true or null -> true (determinate despite missing operand)
        assertEquals("value = false",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("a", false).getNodeState(),
                new String[]{"function", "op", "@a", "'and'", "@b"}).toString());
        assertEquals("value = true",
                FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("a", true).getNodeState(),
                new String[]{"function", "op", "@a", "'or'", "@b"}).toString());
        // true and null -> null (unknown); false or null -> null
        assertNull(FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("a", true).getNodeState(),
                new String[]{"function", "op", "@a", "'and'", "@b"}));
        assertNull(FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty("a", false).getNodeState(),
                new String[]{"function", "op", "@a", "'or'", "@b"}));
    }

    private static org.apache.jackrabbit.oak.api.PropertyState calcOp(
            String aName, long aValue, String bName, long bValue, String operator) {
        return FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty(aName, aValue).setProperty(bName, bValue).getNodeState(),
                new String[]{"function", "op", "@" + aName, "'" + operator + "'", "@" + bName});
    }

    private static org.apache.jackrabbit.oak.api.PropertyState calcOp3(
            String aName, boolean aValue, String bName, boolean bValue, String operator) {
        return FunctionIndexProcessor.tryCalculateValue("x",
                EMPTY_NODE.builder().setProperty(aName, aValue).setProperty(bName, bValue).getNodeState(),
                new String[]{"function", "op", "@" + aName, "'" + operator + "'", "@" + bName});
    }

    private static void checkConvert(String function, String expectedPolishNotation) {
        String p = FunctionIndexProcessor.convertToPolishNotation(function);
        assertEquals(expectedPolishNotation, p);
    }

}
