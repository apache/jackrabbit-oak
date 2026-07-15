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
package org.apache.jackrabbit.j2ee;

import javax.servlet.ServletException;

/**
 * Utility class that links {@link ServletException} with support for
 * the exception chaining mechanism in {@link Throwable}.
 *
 * @see <a href="https://issues.apache.org/jira/browse/JCR-1598">JCR-1598</a>
 */
public class ServletExceptionWithCause extends ServletException {

    /**
     * Serial version UID
     */
    private static final long serialVersionUID = -7201954529718775444L;

    /**
     * Creates a servlet exception with the given message and cause.
     *
     * @param message exception message
     * @param cause cause of the exception
     */
    public ServletExceptionWithCause(String message, Throwable cause) {
        super(message, cause);
        if (getCause() == null) {
            initCause(cause);
        }
    }

}
