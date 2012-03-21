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
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.jackrabbit.mk.api.MicroKernel;

public class QueryEngine {

    public static final String XPATH = "xpath";
    public static final String SQL2 = "sql2";

    private final MicroKernel mk;
    private final ValueFactory vf = new ValueFactory();
    private final SQL2Parser parserSQL2;

    private QueryEngine(MicroKernel mk) {
        this.mk = mk;
        parserSQL2 = new SQL2Parser(vf);
    }

    public static QueryEngine getInstance(MicroKernel mk) {
        return new QueryEngine(mk);
    }

    public Iterator<Row> executeQuery(String language, String query, Map<String, Value> bindings) throws ParseException {
        Query q;
        if (SQL2.equals(language)) {
            q = parserSQL2.parse(query);
        } else if (XPATH.equals(language)) {
            XPathToSQL2Converter converter = new XPathToSQL2Converter();
            String sql2 = converter.convert(query);
            q = parserSQL2.parse(sql2);
        } else {
            throw new ParseException("Unsupported language: " + language, 0);
        }
        q.setMicroKernel(mk);
        if (bindings != null) {
            for (Entry<String, Value> e : bindings.entrySet()) {
                q.bindValue(e.getKey(), e.getValue());
            }
        }
        return q.executeQuery(mk.getHeadRevision());
    }

}
