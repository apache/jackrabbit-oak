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

/**
 * Exception for signaling that the JCR API is not available.
 */
public class JcrApiNotFoundException extends ServletExceptionWithCause {

    /**
     * Serial version UID
     */
    private static final long serialVersionUID = -6439777923943394980L;

    /**
     * Creates an exception to signal that the JCR API is not available.
     *
     * @param e the specific exception that indicates the lack of the JCR API
     */
    public JcrApiNotFoundException(ClassNotFoundException e) {
        super("JCR API (jcr-1.0.jar) not available in the classpath", e);
    }

}
