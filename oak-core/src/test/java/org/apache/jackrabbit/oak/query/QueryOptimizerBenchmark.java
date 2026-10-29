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
package org.apache.jackrabbit.oak.query;

import java.util.Arrays;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Manual, approximate benchmark of Matcher.find(), including matcher allocation.
 * After compiling tests, run from the repository root with:
 * java -cp oak-core/target/test-classes:oak-core/target/classes
 * org.apache.jackrabbit.oak.query.QueryOptimizerBenchmark [iterations]
 */
public final class QueryOptimizerBenchmark {

    private static final int WARMUP_ROUNDS = 3;
    private static final int MEASUREMENT_ROUNDS = 5;

    private QueryOptimizerBenchmark() {
    }

    public static void main(String[] args) {
        int iterations = args.length == 0 ? 50_000 : Integer.parseInt(args[0]);
        if (iterations <= 0) {
            throw new IllegalArgumentException("Iterations must be positive");
        }
        String sql = "select * from [nt:base] where [legacyProperty] = 'hello world'";
        String xpathPrefix = "/jcr:root/content//element(*, cq:Page)[(jcr:contains(., '";
        String xpathSuffix = "'))]";
        String xpathPattern = "\\A(\\s*(?i:explain\\s+)?(?i:measure\\s+)?)"
                + Pattern.quote(xpathPrefix) + "((?:[^']++|'')*+)" + Pattern.quote(xpathSuffix) + "\\z";
        String longXPath = xpathPrefix + "hello '' world ".repeat(256) + xpathSuffix;
        Scenario[] scenarios = {
                new Scenario("literal hit", "legacyProperty", sql),
                new Scenario("literal miss", "missingProperty", sql),
                new Scenario("XPath captures", xpathPattern, "explain " + xpathPrefix + "hello world" + xpathSuffix),
                new Scenario("long XPath captures", xpathPattern, longXPath),
                new Scenario("long XPath suffix miss", xpathPattern, longXPath + " order by @jcr:score"),
                new Scenario("long literal miss", "missingProperty", longXPath)
        };

        System.out.printf(Locale.ROOT, "Java %s; %d iterations/sample; %d warmup + %d measured rounds%n",
                System.getProperty("java.runtime.version"), iterations, WARMUP_ROUNDS, MEASUREMENT_ROUNDS);
        System.out.println("Median ns/find; includes fresh Matcher and, for budgeted input, fresh wrapper/counter.");
        System.out.println("Patterns are precompiled; replacement and budget exhaustion are not measured.");
        System.out.printf(Locale.ROOT, "%-24s %6s %12s %12s %10s %10s%n",
                "Scenario", "Chars", "String", "Budgeted", "Ratio", "Overhead");
        for (Scenario scenario : scenarios) {
            for (int round = 0; round < WARMUP_ROUNDS; round++) {
                measure(scenario, iterations, (round & 1) == 0);
                measure(scenario, iterations, (round & 1) != 0);
            }
            long[] plain = new long[MEASUREMENT_ROUNDS];
            long[] budgeted = new long[MEASUREMENT_ROUNDS];
            for (int round = 0; round < MEASUREMENT_ROUNDS; round++) {
                if ((round & 1) == 0) {
                    plain[round] = measure(scenario, iterations, false);
                    budgeted[round] = measure(scenario, iterations, true);
                } else {
                    budgeted[round] = measure(scenario, iterations, true);
                    plain[round] = measure(scenario, iterations, false);
                }
            }
            Arrays.sort(plain);
            Arrays.sort(budgeted);
            double plainNanos = plain[MEASUREMENT_ROUNDS / 2] / (double) iterations;
            double budgetedNanos = budgeted[MEASUREMENT_ROUNDS / 2] / (double) iterations;
            double ratio = budgetedNanos / plainNanos;
            System.out.printf(Locale.ROOT, "%-24s %6d %12.1f %12.1f %9.2fx %9.1f%%%n",
                    scenario.name, scenario.input.length(), plainNanos, budgetedNanos, ratio, (ratio - 1) * 100);
        }
    }

    private static long measure(Scenario scenario, int iterations, boolean budgeted) {
        long checksum = 0;
        long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            CharSequence input = scenario.input;
            if (budgeted) {
                input = new QueryOptimizer.BudgetedCharSequence(scenario.input,
                        new QueryOptimizer.RegexBudget(
                                QueryOptimizer.CHAR_ACCESSES_PER_CHARACTER * scenario.input.length()));
            }
            Matcher matcher = scenario.pattern.matcher(input);
            if (matcher.find()) {
                checksum += matcher.start() + 1;
            }
        }
        long elapsed = System.nanoTime() - start;
        if (checksum != scenario.expectedChecksum * iterations) {
            throw new AssertionError("Unexpected matching result: " + scenario.name + " " + checksum);
        }
        return elapsed;
    }

    private static final class Scenario {
        final String name;
        final Pattern pattern;
        final String input;
        final long expectedChecksum;

        Scenario(String name, String regex, String input) {
            this.name = name;
            this.pattern = Pattern.compile(regex);
            this.input = input;
            Matcher matcher = pattern.matcher(input);
            this.expectedChecksum = matcher.find() ? matcher.start() + 1 : 0;
        }
    }
}
