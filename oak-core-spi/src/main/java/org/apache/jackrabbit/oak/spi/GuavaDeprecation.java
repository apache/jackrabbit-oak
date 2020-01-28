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
package org.apache.jackrabbit.oak.spi;

import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GuavaDeprecation {

    private static final Logger LOG = LoggerFactory.getLogger(GuavaDeprecation.class);

    private static final String DEFAULT = "warn";

    private static final String TLOGLEVEL = System.getProperty(GuavaDeprecation.class + ".LOGLEVEL", DEFAULT);

    private static String LOGLEVEL;

    static {
        String t;

        switch (TLOGLEVEL.toLowerCase(Locale.ENGLISH)) {
            case "error":
            case "warn":
            case "info":
            case "debug":
                t = TLOGLEVEL.toLowerCase(Locale.ENGLISH);
                break;
            default:
                t = DEFAULT;
                break;
        }

        LOGLEVEL = t;
    }

    private GuavaDeprecation() {
    }

    public static void handleCall(String ticket) throws UnsupportedOperationException {
        handleCall(ticket, null, Collections.emptyList());
    }

    public static void handleCall(String ticket, String className, List<String> allowed) throws UnsupportedOperationException {
        String message = "use of deprecated Guava-related API - this method is going to be removed in future Oak releases - see %s for details";

        switch (LOGLEVEL) {
            case "error":
                if (LOG.isErrorEnabled()) {
                    Exception ex = new Exception("call stack");
                    if (deprecatedCaller(ex, className, allowed)) {
                        LOG.error(String.format(message, ticket), ex);
                    }
                }
                break;
            case "warn":
                if (LOG.isWarnEnabled()) {
                    Exception ex = new Exception("call stack");
                    if (deprecatedCaller(ex, className, allowed)) {
                        LOG.warn(String.format(message, ticket), ex);
                    }
                }
                break;
            case "info":
                if (LOG.isInfoEnabled()) {
                    Exception ex = new Exception("call stack");
                    if (deprecatedCaller(ex, className, allowed)) {
                        LOG.info(String.format(message, ticket), ex);
                    }
                }
                break;
            case "debug":
                if (LOG.isDebugEnabled()) {
                    Exception ex = new Exception("call stack");
                    if (deprecatedCaller(ex, className, allowed)) {
                        LOG.debug(String.format(message, ticket), ex);
                    }
                }
                break;
        }
    }

    public static boolean deprecatedCaller(Exception ex, String className, List<String> allowed) {
        if (allowed == null) {
            return true;
        } else {
            boolean classFound = false;
            for (StackTraceElement el : ex.getStackTrace()) {
                String cn = el.getClassName();
                if (!classFound) {
                    // still looking for the entry
                    classFound = cn.equals(className);
                } else {
                    // still in class checked for?
                    if (cn.equals(className)) {
                        // go one
                    } else {
                        // check caller
                        for (String a : allowed) {
                            if (cn.startsWith(a)) {
                                return false;
                            }
                        }
                    }
                }
            }
            return true;
        }
    }
}
