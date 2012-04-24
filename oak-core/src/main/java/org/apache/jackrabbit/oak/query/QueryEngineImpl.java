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
package org.apache.jackrabbit.oak.query;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.index.Indexer;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.query.index.Filter;
import org.apache.jackrabbit.oak.query.index.QueryIndex;
import org.apache.jackrabbit.oak.query.index.TraversingIndex;
import org.apache.jackrabbit.oak.query.index.QueryIndexProvider.QueryIndexListener;

public class QueryEngineImpl implements QueryEngine, QueryIndexListener {

    static final String SQL2 = "JCR-SQL2";
    private static final String XPATH = "xpath";

    // TODO discuss where to store index config data
    private static final String INDEX_CONFIG_ROOT = "/jcr:system/indexes";

    private final MicroKernel mk;
    private final CoreValueFactory vf;
    private final SQL2Parser parserSQL2;
    private final Indexer indexer;
    private final Map<String, QueryIndex> indexes =
        Collections.synchronizedMap(new HashMap<String, QueryIndex>());

    public QueryEngineImpl(MicroKernel mk, CoreValueFactory valueFactory) {
        this.mk = mk;
        this.vf = valueFactory;
        parserSQL2 = new SQL2Parser(vf);
        indexer = new Indexer(mk, INDEX_CONFIG_ROOT);
    }

    public void init() {
        // TODO the list of index providers should be configurable as well
        indexer.init();
        indexer.addListener(this);
        List<QueryIndex> list = indexer.getQueryIndexes();
        for (QueryIndex qi : list) {
            indexes.put(qi.getIndexName(), qi);
        }
    }

    /**
     * Parse the query (check if it's valid) and get the list of bind variable names.
     *
     * @param statement
     * @param language
     * @return the list of bind variable names
     * @throws ParseException
     */
    @Override
    public List<String> getBindVariableNames(String statement, String language) throws ParseException {
        Query q = parseQuery(statement, language);
        return q.getBindVariableNames();
    }

    private Query parseQuery(String statement, String language) throws ParseException {
        Query q;
        if (SQL2.equals(language)) {
            q = parserSQL2.parse(statement);
        } else if (XPATH.equals(language)) {
            XPathToSQL2Converter converter = new XPathToSQL2Converter();
            String sql2 = converter.convert(statement);
            q = parserSQL2.parse(sql2);
        } else {
            throw new ParseException("Unsupported language: " + language, 0);
        }
        return q;
    }

    @Override
    public ResultImpl executeQuery(String statement, String language, Map<String, CoreValue> bindings) throws ParseException {
        Query q = parseQuery(statement, language);
        q.setMicroKernel(mk);
        if (bindings != null) {
            for (Entry<String, CoreValue> e : bindings.entrySet()) {
                q.bindValue(e.getKey(), e.getValue());
            }
        }
        q.setQueryEngine(this);
        q.prepare();
        return q.executeQuery(mk.getHeadRevision());
    }

    public QueryIndex getBestIndex(Filter filter) {
        QueryIndex best = null;
        double bestCost = Double.MAX_VALUE;
        for (QueryIndex index : getIndexes()) {
            double cost = index.getCost(filter);
            if (cost < bestCost) {
                best = index;
            }
        }
        if (best == null) {
            best = new TraversingIndex(mk);
        }
        return best;
    }

    private List<QueryIndex> getIndexes() {
        // create a copy, so the underlying map can be modified
        return new ArrayList<QueryIndex>(indexes.values());
    }

    @Override
    public void added(QueryIndex index) {
        indexes.put(index.getIndexName(), index);
    }

    @Override
    public void removed(QueryIndex index) {
        indexes.remove(index.getIndexName());
    }

    @Override
    public void close() {
        indexer.removeListener(this);
    }

}
