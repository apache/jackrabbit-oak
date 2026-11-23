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

/**
 * A regex input that charges each character access to a shared budget.
 */
public final class BudgetedCharSequence implements CharSequence {
    private final String text;
    private final RegexBudget budget;

    public BudgetedCharSequence(String text, RegexBudget budget) {
        this.text = text;
        this.budget = budget;
    }

    @Override
    public int length() {
        return text.length();
    }

    @Override
    public char charAt(int index) {
        budget.consume();
        return text.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return new BudgetedCharSequence(text.substring(start, end), budget);
    }

    @Override
    public String toString() {
        return text;
    }
}
