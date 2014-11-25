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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

public class TestUtil {

    static void useV2(NodeBuilder idxNb) {
        idxNb.setProperty(LuceneIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());
    }

    static void useV2(Tree idxTree) {
        idxTree.setProperty(LuceneIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());
    }

    public static NodeBuilder newLuceneIndexDefinitionV2(
            @Nonnull NodeBuilder index, @Nonnull String name,
            @Nullable Set<String> propertyTypes) {
        NodeBuilder nb = LuceneIndexHelper.newLuceneIndexDefinition(index, name, propertyTypes, null, null, null);
        useV2(nb);
        return nb;
    }

}
