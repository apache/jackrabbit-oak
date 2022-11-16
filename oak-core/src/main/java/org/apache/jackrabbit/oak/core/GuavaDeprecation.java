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
package org.apache.jackrabbit.oak.core;

import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GuavaDeprecation {

    private static final Logger LOG = LoggerFactory.getLogger(GuavaDeprecation.class);

    private static final String DEFAULT = "info";

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
        String message = "use of deprecated Guava-related API - this method is going to be removed in future Oak releases - see %s for details";

        switch (LOGLEVEL) {
            case "error":
                if (LOG.isErrorEnabled()) {
                    LOG.error(String.format(message, ticket), new Exception("call stack"));
                }
                break;
            case "warn":
                if (LOG.isWarnEnabled()) {
                    LOG.warn(String.format(message, ticket), new Exception("call stack"));
                }
                break;
            case "info":
                if (LOG.isInfoEnabled()) {
                    LOG.info(String.format(message, ticket), new Exception("call stack"));
                }
                break;
            case "debug":
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format(message, ticket), new Exception("call stack"));
                }
                break;
        }
    }

    // for testing
    public static String setLogLevel(String level) {
        String before = LOGLEVEL;
        LOGLEVEL = level;
        return before;
    }
}
