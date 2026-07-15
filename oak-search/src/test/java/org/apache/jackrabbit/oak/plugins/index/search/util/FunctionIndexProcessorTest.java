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
    }

    private static void checkConvert(String function, String expectedPolishNotation) {
        String p = FunctionIndexProcessor.convertToPolishNotation(function);
        assertEquals(expectedPolishNotation, p);
    }

}
