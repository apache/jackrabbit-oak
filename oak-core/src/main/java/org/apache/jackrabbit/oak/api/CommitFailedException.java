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
package org.apache.jackrabbit.oak.api;

/**
 * Main exception thrown by methods defined on the {@code ContentSession}
 * interface indicating that committing a given set of changes failed.
 */
public class CommitFailedException extends Exception {

    /** Serial version UID */
    private static final long serialVersionUID = 2727602333350620918L;

    private final String type;

    private final int code;

    public CommitFailedException(
            String type, int code, String message, Throwable cause) {
        super(String.format("Oak%s%04d: %s", type, code, message), cause);
        this.type = type;
        this.code = code;
    }

    public CommitFailedException(String type, int code, String message) {
        this(type, code, message, null);
    }

    public boolean hasType(String type) {
        return this.type.equals(type);
    }

    public boolean hasCode(int code) {
        return this.code == code;
    }

    public boolean hasTypeAndCode(String type, int code) {
        return hasType(type) && hasCode(code);
    }

    public String getType() {
        return type;
    }

    public int getCode() {
        return code;
    }

}
