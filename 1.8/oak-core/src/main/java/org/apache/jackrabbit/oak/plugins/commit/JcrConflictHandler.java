/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.commit;

import org.apache.jackrabbit.oak.spi.commit.CompositeConflictHandler;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandlers;

import com.google.common.collect.ImmutableList;

/**
 * Utility class providing conflict handlers used for JCR.
 */
public final class JcrConflictHandler {

    /**
     * The conflict handler is a composite of
     * {@link JcrLastModifiedConflictHandler}, {@link ChildOrderConflictHandler}
     * and {@link AnnotatingConflictHandler}.
     */
    public static CompositeConflictHandler createJcrConflictHandler() {
        return new CompositeConflictHandler(ImmutableList.of(
                new JcrLastModifiedConflictHandler(),
                ConflictHandlers.wrap(new ChildOrderConflictHandler()),
                new AnnotatingConflictHandler()));
    }

    private JcrConflictHandler() {
    }
}
