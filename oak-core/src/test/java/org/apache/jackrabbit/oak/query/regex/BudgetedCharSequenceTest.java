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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

public class BudgetedCharSequenceTest {

    @Test
    public void onlyCharacterAccessConsumesBudget() {
        BudgetedCharSequence text = new BudgetedCharSequence("abc", new RegexBudget(2));
        assertEquals(3, text.length());
        assertEquals("abc", text.toString());
        assertEquals('b', text.charAt(1));
        assertEquals('b', text.charAt(1));
        assertThrows(RegexBudgetExceededException.class, () -> text.charAt(1));
        assertEquals(3, text.length());
        assertEquals("abc", text.toString());
    }

    @Test
    public void subsequencesAndInputsShareBudget() {
        RegexBudget budget = new RegexBudget(3);
        BudgetedCharSequence text = new BudgetedCharSequence("abcd", budget);
        CharSequence sub = text.subSequence(1, 4);
        CharSequence nested = sub.subSequence(1, 2);
        BudgetedCharSequence other = new BudgetedCharSequence("xyz", budget);
        assertEquals("bcd", sub.toString());
        assertEquals(3, sub.length());
        assertEquals("c", nested.toString());
        assertEquals('b', sub.charAt(0));
        assertEquals('c', nested.charAt(0));
        assertEquals('x', other.charAt(0));
        assertThrows(RegexBudgetExceededException.class, () -> text.charAt(0));
        assertThrows(RegexBudgetExceededException.class, () -> sub.charAt(0));
        assertThrows(RegexBudgetExceededException.class, () -> nested.charAt(0));
        assertThrows(RegexBudgetExceededException.class, () -> other.charAt(0));
    }

    @Test
    public void emptyInputNeedsNoBudget() {
        BudgetedCharSequence text = new BudgetedCharSequence("", new RegexBudget(0));
        assertEquals(0, text.length());
        assertEquals("", text.subSequence(0, 0).toString());
        assertTrue(Pattern.compile("").matcher(text).matches());
    }

    @Test
    public void regexCapturesAndReplacement() {
        Matcher matcher = Pattern.compile("(a)(b)")
                .matcher(new BudgetedCharSequence("ab ab", new RegexBudget(100)));
        assertEquals("ba ba", matcher.replaceAll("$2$1"));
    }

    @Test
    public void regexBacktrackingExhaustsBudget() {
        Pattern pattern = Pattern.compile("a+a+a+a+a+b");
        BudgetedCharSequence text = new BudgetedCharSequence("a".repeat(50), new RegexBudget(100));
        assertThrows(RegexBudgetExceededException.class, () -> pattern.matcher(text).find());
        assertFalse(pattern.matcher(new BudgetedCharSequence("abc", new RegexBudget(100))).find());
    }
}
