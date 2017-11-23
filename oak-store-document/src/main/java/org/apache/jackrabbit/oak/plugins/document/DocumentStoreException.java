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
package org.apache.jackrabbit.oak.plugins.document;

import com.google.common.collect.Lists;

/**
 * <code>DocumentStoreException</code> is a runtime exception for
 * {@code DocumentStore} implementations to signal unexpected problems like
 * a communication exception.
 */
public class DocumentStoreException extends RuntimeException {

    private static final long serialVersionUID = 634445274043721284L;

    public DocumentStoreException(String message) {
        super(message);
    }

    public DocumentStoreException(Throwable cause) {
        super(cause);
    }

    public DocumentStoreException(String message, Throwable cause) {
        super(message, cause);
    }

    public static DocumentStoreException convert(Throwable t) {
        return convert(t, t.getMessage());
    }

    public static DocumentStoreException convert(Throwable t, String msg) {
        if (t instanceof DocumentStoreException) {
            return (DocumentStoreException) t;
        } else {
            return new DocumentStoreException(msg, t);
        }
    }

    public static DocumentStoreException convert(Throwable t,
                                                 Iterable<String> ids) {
        String msg = t.getMessage() + " " + Lists.newArrayList(ids);
        return convert(t, msg);
    }
}
