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
package org.apache.jackrabbit.oak.spi.query.fulltext;

/**
 * Test-only access to the protected compatibility mode switch of {@link VectorQuery}.
 */
public final class VectorQueryTestUtil {

    private VectorQueryTestUtil() {
    }

    /**
     * Sets the experimental compatibility mode system property ({@code null} clears it) and re-initializes
     * {@link VectorQuery}. Returns the previous property value so it can be restored.
     */
    public static String setCompatibilityMode(String value) {
        String previous = value == null
                ? System.clearProperty(VectorQuery.EXPERIMENTAL_COMPATIBILITY_MODE_KEY)
                : System.setProperty(VectorQuery.EXPERIMENTAL_COMPATIBILITY_MODE_KEY, value);
        VectorQuery.reInitializeCompatibilityMode();
        return previous;
    }
}
