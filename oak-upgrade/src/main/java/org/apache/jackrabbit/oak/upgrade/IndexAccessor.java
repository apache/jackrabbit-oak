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
package org.apache.jackrabbit.oak.upgrade;

import org.apache.jackrabbit.core.RepositoryContext;
import org.apache.jackrabbit.core.RepositoryImpl;
import org.apache.jackrabbit.core.SearchManager;
import org.apache.jackrabbit.core.query.QueryHandler;
import org.apache.jackrabbit.core.query.lucene.SearchIndex;
import org.apache.lucene.index.IndexReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.io.IOException;
import java.lang.reflect.Method;

public final class IndexAccessor {

    private static final Logger logger = LoggerFactory.getLogger(IndexAccessor.class);

    private IndexAccessor() {
    }

    public static IndexReader getReader(RepositoryContext ctx) throws RepositoryException, IOException {
        RepositoryImpl repo = ctx.getRepository();
        SearchManager searchMgr = null;
        try {
            Method gsm = RepositoryImpl.class.getDeclaredMethod("getSearchManager", String.class);
            gsm.setAccessible(true);
            searchMgr = (SearchManager) gsm.invoke(repo, ctx.getRepositoryConfig().getDefaultWorkspaceName());
        } catch (Exception ex) {
            logger.error("getting search manager", ex);
        }
        if (searchMgr == null) {
            return null;
        }
        QueryHandler handler = searchMgr.getQueryHandler();
        SearchIndex index = (SearchIndex) handler;
        return index.getIndexReader();
    }

}
