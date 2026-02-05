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
package org.apache.jackrabbit.oak.plugins.index.optimizer;

import org.junit.Test;

public class FunctionNameConverterTest {
    @Test
    public void testFormatNameSQL2() {
        checkConvert("function*upper*@data", "upperData", false);
        checkConvert("function*lower*@test/data", "lowerData", false);
        checkConvert("function*lower*@:name", "lowerName", false);
        checkConvert("function*lower*@:localname", "lowerLocalname", false);
        checkConvert("function*length*@test/data", "lengthData", false);
        checkConvert("function*length*@:name", "lengthName", false);
        checkConvert("function*@:path", "path", false);
        checkConvert("function*length*@:path", "lengthPath", false);
        checkConvert("function*lower*upper*@test/data", "lowerUpperData", false);
        checkConvert("function*coalesce*@jcr:content/foo2*@jcr:content/foo", "coalesceFoo2Foo", false);
        checkConvert("function*coalesce*@jcr:content/foo2*lower*@jcr:content/foo",
            "coalesceFoo2LowerFoo", false);
        checkConvert("function*coalesce*@jcr:content/foo2*coalesce*@jcr:content/foo*lower*@:name",
            "coalesceFoo2CoalesceFooLowerName", false);
        checkConvert(
            "function*coalesce*coalesce*@jcr:content/foo2*@jcr:content/foo*coalesce*@a:b*@c:d",
            "coalesceCoalesceFoo2FooCoalesceBD", false);
        checkConvert("function*first*@jcr:content/foo2", "firstFoo2", false);
    }

    @Test
    public void testFormatNameXPath() {
        checkConvert("function*upper*@data", "upperCaseData", true);
        checkConvert("function*lower*@test/data", "lowerCaseData", true);
        checkConvert("function*lower*@:name", "lowerCaseName", true);
        checkConvert("function*lower*@:localname", "lowerCaseLocalname", true);
        checkConvert("function*length*@test/data", "stringLengthData", true);
        checkConvert("function*length*@:name", "stringLengthName", true);
        checkConvert("function*@:path", "path", true);
        checkConvert("function*length*@:path", "stringLengthPath", true);
        checkConvert("function*lower*upper*@test/data", "lowerCaseUpperCaseData", true);
        checkConvert("function*coalesce*@jcr:content/foo2*@jcr:content/foo", "coalesceFoo2Foo", true);
        checkConvert("function*coalesce*@jcr:content/foo2*lower*@jcr:content/foo",
            "coalesceFoo2LowerCaseFoo", true);
        checkConvert("function*coalesce*@jcr:content/foo2*coalesce*@jcr:content/foo*lower*@:name",
            "coalesceFoo2CoalesceFooLowerCaseName", true);
        checkConvert(
            "function*coalesce*coalesce*@jcr:content/foo2*@jcr:content/foo*coalesce*@a:b*@c:d",
            "coalesceCoalesceFoo2FooCoalesceBD", true);
        checkConvert("function*first*@jcr:content/foo2", "firstFoo2", true);
    }


    private static void checkConvert(String input, String expected, boolean isXPath) {
        String actual = FunctionNameConverter.apply(input, isXPath);
        assert expected.equals(actual);
    }
}
