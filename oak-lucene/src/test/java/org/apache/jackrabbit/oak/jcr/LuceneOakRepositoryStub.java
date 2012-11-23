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
package org.apache.jackrabbit.oak.jcr;

import java.util.Properties;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.plugins.index.lucene.LowCostLuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexHookProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneInitializerHelper;

public class LuceneOakRepositoryStub extends OakRepositoryStubBase {

    public LuceneOakRepositoryStub(Properties settings)
            throws RepositoryException {
        super(settings);
    }

    @Override
    protected void preCreateRepository(Jcr jcr) {
        jcr.with(new LuceneInitializerHelper("/oak:index/luceneGlobal"))
                .with(new LowCostLuceneIndexProvider())
                .with(new LuceneIndexHookProvider());
    }
}
