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
package org.apache.jackrabbit.oak.plugins.index.lucene.util;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

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
                "fn:lower-case(fn:upper-case(test/@data))", 
                "function*lower*upper*@test/data");
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
                "lower(upper([test/data]))", 
                "function*lower*upper*@test/data");
        // the ']' character is escaped as ']]'
        checkConvert(
                "[strange[0]]]", 
                "function*@strange[0]");
    }

    private static void checkConvert(String function, String expectedPolishNotation) {
        String p = FunctionIndexProcessor.convertToPolishNotation(function);
        assertEquals(expectedPolishNotation, p);
    }

}
