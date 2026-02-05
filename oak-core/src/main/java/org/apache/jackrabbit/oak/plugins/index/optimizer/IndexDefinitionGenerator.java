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
package org.apache.jackrabbit.oak.plugins.index.optimizer;

import java.text.ParseException;

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.json.Base64BlobSerializer;
import org.apache.jackrabbit.oak.json.JsonSerializer;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.query.NodeStateNodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.Query;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.QueryImpl;
import org.apache.jackrabbit.oak.query.SQL2Parser;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.stats.QueryStatsData;
import org.apache.jackrabbit.oak.query.stats.QueryStatsData.QueryExecutionStats;
import org.apache.jackrabbit.oak.query.xpath.XPathToSQL2Converter;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class IndexDefinitionGenerator {

    public static String generateIndexDefinition(String language, String queryStatement) {
        IndexConfigGenerator gen = new IndexConfigGenerator();
        try {
            gen.process(queryStatement, language);
            NodeState state = gen.getIndexConfig();
            JsopBuilder json = new JsopBuilder();
            json.object();
            json.key("index");
            String filter = "{\"properties\":[\"*\", \"-:childOrder\"],\"nodes\":[\"*\", \"-:*\"]}";;
            JsonSerializer serializer = new JsonSerializer(json, filter, new Base64BlobSerializer());
            serializer.serialize(state);
            json.endObject();
            return JsopBuilder.prettyPrint(json.toString());
        } catch (Throwable e) {
            // ignore
            return "error: " + e.toString();
        }
    }

    public static String generateIndexDefinition2(NodeState rootState, String language, String queryStatement) {
        NamePathMapper namePathMapper = NamePathMapper.DEFAULT;
        NodeTypeInfoProvider nodeTypes = new NodeStateNodeTypeInfoProvider(rootState);
        QueryEngineSettings settings = new QueryEngineSettings();
        QueryStatsData data = new QueryStatsData("", "");
        QueryExecutionStats stats = data.new QueryExecutionStats();
        SQL2Parser parser = new SQL2Parser(namePathMapper, nodeTypes, settings, stats);
        try {
            Query query;
            if ("xpath".equals(language)) {
                XPathToSQL2Converter converter = new XPathToSQL2Converter();
                String sql2 = converter.convert(queryStatement);
                query = parser.parse(sql2);
            } else if ("JCR-SQL2".equals(language)) {
                query = parser.parse(queryStatement);
            } else if ("sql".equals(language)) {
                parser.setSupportSQL1(true);
                query = parser.parse(queryStatement);
            } else {
                return "";
            }
            try {
                query.init();
            } catch (Exception e) {
                ParseException e2 = new ParseException(query.getStatement() + ": " + e.getMessage(), 0);
                e2.initCause(e);
                throw e2;
            }
            query.prepare();
            Filter filter = ((QueryImpl) query).createFilter(true);
            return "filter: " + filter.toString();
        } catch (Throwable e) {
            // ignore
            return "error: " + e.toString();
        }
    }
}
