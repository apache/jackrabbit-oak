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
package org.apache.jackrabbit.oak.spi.query;

import java.io.Closeable;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.commit.CommitHook;

/**
 * An index is a lookup mechanism. It typically uses a tree to store data. It
 * updates the tree whenever a node was changed. The index is updated
 * automatically.
 */
public interface Index extends CommitHook, Closeable {

    /**
     * Get the the index definition. This contains the name, type, uniqueness
     * and other properties.
     * 
     * @return the index definition
     */
    @Nonnull
    IndexDefinition getDefinition();

}
