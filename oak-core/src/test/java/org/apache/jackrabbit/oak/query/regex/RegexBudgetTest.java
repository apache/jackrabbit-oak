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
package org.apache.jackrabbit.oak.query.regex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class RegexBudgetTest {

    @Test
    public void exactLimitAndRepeatedExhaustion() {
        RegexBudget budget = new RegexBudget(2);
        budget.consume();
        budget.consume();
        assertThrows(RegexBudgetExceededException.class, budget::consume);
        assertThrows(RegexBudgetExceededException.class, budget::consume);
        assertEquals(2, budget.getLimit());
    }

    @Test
    public void zeroBudgetThrowsWithoutStackTrace() {
        RegexBudget budget = new RegexBudget(0);
        RegexBudgetExceededException failure =
                assertThrows(RegexBudgetExceededException.class, budget::consume);
        assertEquals(0, failure.getStackTrace().length);
        failure.fillInStackTrace();
        assertEquals(0, failure.getStackTrace().length);
    }
}
