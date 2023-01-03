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
package org.apache.jackrabbit.oak.jcr.util;

import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.jackrabbit.test.AbstractJCRTest;
import org.apache.jackrabbit.test.JCRTestResult;
import org.junit.Assume;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

/**
 * Evaluates the system properties "known.issues" and "known.issues.override" to skip failing for certain test method failures.
 * This is the JUnit4 equivalent of {@link JCRTestResult} which is used by the JUnit3 base class {@link AbstractJCRTest}.
 */
public class KnownIssuesIgnoreRule implements MethodRule {

    /**
     * Set of Strings that identify the test methods that currently fails but
     * are recognized as known issues. Those will not be reported as errors.
     */
    private static final Set<String> KNOWN_ISSUES = tokenize("known.issues");
    private static final Set<String> KNOWN_ISSUES_OVERRIDE = tokenize("known.issues.override");

    // copied from https://github.com/apache/jackrabbit/blob/ed3124e5fe223dada33ce6ddf53bc666063c3f2f/jackrabbit-jcr-tests/src/main/java/org/apache/jackrabbit/test/JCRTestResult.java#L161-L177
    /**
     * Takes the named system property and returns the set of string tokens
     * in the property value. Returns an empty set if the named property does
     * not exist.
     *
     * @param name name of the system property
     * @return set of string tokens
     */
    private static Set<String> tokenize(String name) {
        Set<String> tokens = new HashSet<String>();
        StringTokenizer tokenizer =
            new StringTokenizer(System.getProperty(name, ""));
        while (tokenizer.hasMoreTokens()) {
            tokens.add(tokenizer.nextToken());
        }
        return tokens;
    }
    
    // similar to https://github.com/apache/jackrabbit/blob/ed3124e5fe223dada33ce6ddf53bc666063c3f2f/jackrabbit-jcr-tests/src/main/java/org/apache/jackrabbit/test/JCRTestResult.java#L179-L215
    /**
     * Checks if a variation of the name of the given test case is included
     * in the given set of token. The tested variations are:
     * <ul>
     *   <li>package name</li>
     *   <li>non-qualified class name</li>
     *   <li>fully qualified class name</li>
     *   <li>non-qualified method name</li>
     *   <li>class-qualified method name</li>
     *   <li>fully-qualified method name</li>
     * </ul>
     *
     * @param tokens set of string tokens
     * @param className the fully qualified class name
     * @param methodName the method name
     * @return <code>true</code> if the test case name is included,
     *         <code>false</code> otherwise
     */
    private static boolean contains(Set<String> tokens, String className, String methodName) {
        int i = className.lastIndexOf('.');
        if (i >= 0) {
            String packageName = className.substring(0, i);
            String shortName = className.substring(i + 1);
            return tokens.contains(packageName)
                || tokens.contains(shortName)
                || tokens.contains(className)
                || tokens.contains(methodName)
                || tokens.contains(shortName + "#" + methodName)
                || tokens.contains(className + "#" + methodName);
        } else {
            return tokens.contains(className)
                || tokens.contains(methodName)
                || tokens.contains(className + "#" + methodName);
        }
    }
   
    // similar to https://github.com/apache/jackrabbit/blob/ed3124e5fe223dada33ce6ddf53bc666063c3f2f/jackrabbit-jcr-tests/src/main/java/org/apache/jackrabbit/test/JCRTestResult.java#L217-L231
    private static boolean isKnownIssue(String className, String methodName) {
        return contains(KNOWN_ISSUES, className, methodName)
                && !contains(KNOWN_ISSUES_OVERRIDE, className, methodName);
    }


    @Override
    public Statement apply(Statement base, FrameworkMethod method, Object target) {
        return new IgnorableStatement(base, method.getDeclaringClass().getName(), method.getName());
    }

    private class IgnorableStatement extends Statement {

        private final Statement base;
        private final String className;
        private final String methodName;

        public IgnorableStatement(Statement base, String className, String methodName) {
            this.base = base;
            this.className = className;
            this.methodName = methodName;
        }

        @Override
        public void evaluate() throws Throwable {
            Assume.assumeTrue("Test is ignored through system property 'known.issues'!", !isKnownIssue(className, methodName));
            base.evaluate();
        }
    }

}
