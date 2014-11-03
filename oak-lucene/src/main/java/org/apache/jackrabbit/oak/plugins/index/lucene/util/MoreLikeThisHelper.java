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
package org.apache.jackrabbit.oak.plugins.index.lucene.util;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;

/**
 * Helper class for generating a {@link org.apache.lucene.queries.mlt.MoreLikeThisQuery} from the native query <code>String</code>
 */
public class MoreLikeThisHelper {

    public static Query getMoreLikeThis(IndexReader reader, Analyzer analyzer, String mltQueryString) {
        Query moreLikeThisQuery = null;
        MoreLikeThis mlt = new MoreLikeThis(reader);
        mlt.setAnalyzer(analyzer);
        try {
            String text = null;
            String[] fields = {};
            for (String param : mltQueryString.split("&")) {
                String[] keyValuePair = param.split("=");
                if (keyValuePair.length != 2 || keyValuePair[0] == null || keyValuePair[1] == null) {
                    throw new RuntimeException("Unparsable native Lucene MLT query: " + mltQueryString);
                } else {
                    if ("stream.body".equals(keyValuePair[0])) {
                        text = keyValuePair[1];
                    } else if ("mlt.fl".equals(keyValuePair[0])) {
                        fields = keyValuePair[1].split(",");
                    } else if ("mlt.mindf".equals(keyValuePair[0])) {
                        mlt.setMinDocFreq(Integer.parseInt(keyValuePair[1]));
                    } else if ("mlt.mintf".equals(keyValuePair[0])) {
                        mlt.setMinTermFreq(Integer.parseInt(keyValuePair[1]));
                    } else if ("mlt.boost".equals(keyValuePair[0])) {
                        mlt.setBoost(Boolean.parseBoolean(keyValuePair[1]));
                    } else if ("mlt.qf".equals(keyValuePair[0])) {
                        mlt.setBoostFactor(Float.parseFloat(keyValuePair[1]));
                    } else if ("mlt.maxdf".equals(keyValuePair[0])) {
                        mlt.setMaxDocFreq(Integer.parseInt(keyValuePair[1]));
                    } else if ("mlt.maxdfp".equals(keyValuePair[0])) {
                        mlt.setMaxDocFreqPct(Integer.parseInt(keyValuePair[1]));
                    } else if ("mlt.maxntp".equals(keyValuePair[0])) {
                        mlt.setMaxNumTokensParsed(Integer.parseInt(keyValuePair[1]));
                    } else if ("mlt.maxqt".equals(keyValuePair[0])) {
                        mlt.setMaxQueryTerms(Integer.parseInt(keyValuePair[1]));
                    } else if ("mlt.maxwl".equals(keyValuePair[0])) {
                        mlt.setMaxWordLen(Integer.parseInt(keyValuePair[1]));
                    } else if ("mlt.minwl".equals(keyValuePair[0])) {
                        mlt.setMinWordLen(Integer.parseInt(keyValuePair[1]));
                    }
                }
            }
            if (text != null) {
                if (FieldNames.PATH.equals(fields[0])) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    TermQuery q = new TermQuery(new Term(FieldNames.PATH, text));
                    TopDocs top = searcher.search(q, 1);
                    if (top.totalHits == 0) {
                        mlt.setFieldNames(fields);
                        moreLikeThisQuery = mlt.like(new StringReader(text), mlt.getFieldNames()[0]);
                    } else{
                        ScoreDoc d = top.scoreDocs[0];
                        Document doc = reader.document(d.doc);
                        List<String> fieldNames = new ArrayList<String>();
                        for (IndexableField f : doc.getFields()) {
                            if (!FieldNames.PATH.equals(f.name())) {
                                fieldNames.add(f.name());
                            }
                        }
                        String[] docFields = fieldNames.toArray(new String[fieldNames.size()]);
                        mlt.setFieldNames(docFields);
                        moreLikeThisQuery = mlt.like(d.doc);
                    }
                } else {
                    mlt.setFieldNames(fields);
                    moreLikeThisQuery = mlt.like(new StringReader(text), mlt.getFieldNames()[0]);
                }
            }
            return moreLikeThisQuery;
        } catch (Exception e) {
            throw new RuntimeException("could not handle MLT query " + mltQueryString);
        }
    }
}
