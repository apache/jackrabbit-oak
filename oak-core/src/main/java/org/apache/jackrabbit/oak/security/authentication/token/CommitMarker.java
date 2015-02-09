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
package org.apache.jackrabbit.oak.security.authentication.token;

import java.util.Collections;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;

/**
 * Marker object to help the {@link org.apache.jackrabbit.oak.security.authentication.token.TokenValidatorProvider.TokenValidator}
 * identifying if login tokens have been issued and updated by the {@code TokenProvider}
 * implementation provided by this package and not through regular write
 * operations on the Oak API which doesn't enforce the protected status
 * of the login tokens as defined by this implementation.
 */
final class CommitMarker {

    private static final String KEY = CommitMarker.class.getName();

    private static final CommitMarker INSTANCE = new CommitMarker();

    static Map<String, Object> asCommitAttributes() {
        return Collections.<String, Object>singletonMap(CommitMarker.KEY, CommitMarker.INSTANCE);
    }

    static boolean isValidCommitInfo(@Nonnull CommitInfo commitInfo) {
        return CommitMarker.INSTANCE == commitInfo.getInfo().get(CommitMarker.KEY);
    }

    private CommitMarker() {}
}