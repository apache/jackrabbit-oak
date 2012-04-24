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

import java.io.InputStream;

/**
 * An immutable, typed scalar value.
 *
 * TODO: remove if really not needed any more
 */
public interface Scalar {
    enum Type {BOOLEAN, LONG, DOUBLE, BINARY, STRING, NULL}

    /**
     * Returns the value type.
     * <p>
     *
     * @return value type
     */
    Type getType();

    boolean getBoolean();

    long getLong();

    double getDouble();

    InputStream getInputStream();

    String getString();

}
