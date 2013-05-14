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
package org.apache.jackrabbit.oak.core;

import javax.annotation.Nullable;

/**
 * TreeTypeProvider... TODO
 */
public interface TreeTypeProvider {

    // regular trees
    int TYPE_DEFAULT = 1;
    // version store(s) content
    int TYPE_VERSION = 2;
    // access control content
    int TYPE_AC = 4;
    // hidden trees
    int TYPE_HIDDEN = 8;

    TreeTypeProvider EMPTY = new TreeTypeProvider() {
        @Override
        public int getType(@Nullable ImmutableTree tree) {
            return TYPE_DEFAULT;
        }
    };

    int getType(ImmutableTree tree);
}
