/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.jackrabbit.oak.query;


import org.apache.jackrabbit.oak.api.QueryEngine;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryInformationLogger {
    
    private static final Logger INTERNAL_QUERIES_LOG = LoggerFactory.getLogger(QueryInformationLogger.class.getName() + ".internalQuery");
    private static final Logger EXTERNAL_QUERIES_LOG = LoggerFactory.getLogger(QueryInformationLogger.class.getName() + ".externalQuery");
    
    private QueryInformationLogger() {
        // prevent instantiation
    }
    
    /**
     * Logs the caller of the query based on the callstack. To refine the result, all stack frames
     * are ignored, for which the entry (consisting of package name, classname and method name) starts
     * with one of the entries provided by ignoredClassNames.
     * 
     * @param statement the query statement
     * @param ignoredClassNames entries to be ignored. If empty, the method is a no-op.
     */
    public static void logCaller(@NotNull String statement, @NotNull String[] ignoredClassNames) {
        
        if (ignoredClassNames.length == 0) {
            return;
        }
        
        if (isOakInternalQuery(statement)) {
            INTERNAL_QUERIES_LOG.debug("Oak-internal query, query=[{}]", statement);
            return;
        }
        
        String callingClass = getInvokingClass(ignoredClassNames);
        if (callingClass.isEmpty()) {
            // If all stackframes are covered by the ignoredClassNames, it's something which is
            // explicitly wanted not to be logged along other queries for which this is not true.
            INTERNAL_QUERIES_LOG.debug("Oak-internal query, query=[{}]", statement);
            if (INTERNAL_QUERIES_LOG.isTraceEnabled()) {
                try {
                    throw new RuntimeException("requested stacktrace");
                } catch (RuntimeException e) {
                    INTERNAL_QUERIES_LOG.trace("providing requested stacktrace", e);
                }
            }
        } else {
            EXTERNAL_QUERIES_LOG.info("query=[{}], caller=[{}]",statement, callingClass);
            if (EXTERNAL_QUERIES_LOG.isDebugEnabled()) {
                try {
                    throw new RuntimeException("requested stacktrace");
                } catch (RuntimeException e) {
                    EXTERNAL_QUERIES_LOG.debug("providing requested stacktrace", e);
                }
            }
        }
    }
    
    /**
     * Retrieves the calling class and method from the call stack; this is determined by unwinding
     * the stack until it finds a combination of full qualified classname + method (separated by ".") which
     * do not start with any of the values provided by the ignoredJavaPackages parameters.
     * 
     * @param ignoredJavaPackages
     * @return the calling class; if all classes of the call trace are in packages which are ignored,
     *   an empty string is returned.
     */
    private static String getInvokingClass(String[] ignoredJavaPackages) {
        
        // With java9 we would use https://docs.oracle.com/javase/9/docs/api/java/lang/StackWalker.html
        final StackTraceElement[] callStack = Thread.currentThread().getStackTrace();
        
        for (StackTraceElement stackFrame : callStack) {
            boolean foundMatch = false;
            for (String packageName : ignoredJavaPackages) {
                final String classMethod = stackFrame.getClassName() + "." + stackFrame.getMethodName();
                if (classMethod.startsWith(packageName)) {
                    foundMatch = true;
                    continue;
                }
            }
            if (!foundMatch) {
                return stackFrame.getClassName() + "." + stackFrame.getMethodName();
            }
        }
        return ""; 
    }
    
    
    /**
     * Check if statement is explicitly marked to be internal
     * @param statement the JCR query statement to check
     * @return true the query is marked to be an internal query
     */
    private static boolean isOakInternalQuery(@NotNull String statement) {
        return statement.endsWith(QueryEngine.INTERNAL_SQL2_QUERY);
    }
    
    
    
    
    
    

}
