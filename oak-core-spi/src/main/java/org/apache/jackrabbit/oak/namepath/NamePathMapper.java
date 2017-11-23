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
package org.apache.jackrabbit.oak.namepath;

import java.util.Collections;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

/**
 * The {@code NamePathMapper} interface combines {@code NameMapper} and
 * {@code PathMapper}.
 */
public interface NamePathMapper extends NameMapper, PathMapper {

    NamePathMapper DEFAULT = new Default();

    /**
     * Default implementation that doesn't perform any conversions for cases
     * where a mapper object only deals with oak internal names and paths.
     */
    class Default implements NamePathMapper {

        @Override
        public String getOakNameOrNull(@Nonnull String jcrName) {
            return jcrName;
        }

        @Nonnull
        @Override
        public String getOakName(@Nonnull String jcrName) throws RepositoryException {
            return jcrName;
        }

        @Nonnull
        @Override
        public Map<String, String> getSessionLocalMappings() {
            return Collections.emptyMap();
        }

        @Nonnull
        @Override
        public String getJcrName(@Nonnull String oakName) {
            return oakName;
        }

        @Override
        public String getOakPath(String jcrPath) {
            return jcrPath;
        }

        @Nonnull
        @Override
        public String getJcrPath(String oakPath) {
            return oakPath;
        }
    }
}