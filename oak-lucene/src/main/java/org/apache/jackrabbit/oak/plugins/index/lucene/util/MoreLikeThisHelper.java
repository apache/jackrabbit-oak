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
import java.util.Map;

import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.MoreLikeThisHelperUtil;
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
            Map<String, String> paramMap = MoreLikeThisHelperUtil.getParamMapFromMltQuery(mltQueryString);
            String text = null;
            String[] fields = {};
            for (String key : paramMap.keySet()) {
                String value = paramMap.get(key);
                if (MoreLikeThisHelperUtil.MLT_STREAM_BODY.equals(key)) {
                    text = value;
                } else if (MoreLikeThisHelperUtil.MLT_FILED.equals(key)) {
                    fields = value.split(",");
                } else if (MoreLikeThisHelperUtil.MLT_MIN_DOC_FREQ.equals(key)) {
                    mlt.setMinDocFreq(Integer.parseInt(value));
                } else if (MoreLikeThisHelperUtil.MLT_MIN_TERM_FREQ.equals(key)) {
                    mlt.setMinTermFreq(Integer.parseInt(value));
                } else if (MoreLikeThisHelperUtil.MLT_BOOST.equals(key)) {
                    mlt.setBoost(Boolean.parseBoolean(value));
                } else if (MoreLikeThisHelperUtil.MLT_BOOST_FACTOR.equals(key)) {
                    mlt.setBoostFactor(Float.parseFloat(value));
                } else if (MoreLikeThisHelperUtil.MLT_MAX_DOC_FREQ.equals(key)) {
                    mlt.setMaxDocFreq(Integer.parseInt(value));
                } else if (MoreLikeThisHelperUtil.MLT_MAX_DOC_FREQ_PCT.equals(key)) {
                    mlt.setMaxDocFreqPct(Integer.parseInt(value));
                } else if (MoreLikeThisHelperUtil.MLT_MAX_NUM_TOKENS_PARSED.equals(key)) {
                    mlt.setMaxNumTokensParsed(Integer.parseInt(value));
                } else if (MoreLikeThisHelperUtil.MLT_MAX_QUERY_TERMS.equals(key)) {
                    mlt.setMaxQueryTerms(Integer.parseInt(value));
                } else if (MoreLikeThisHelperUtil.MLT_MAX_WORD_LENGTH.equals(key)) {
                    mlt.setMaxWordLen(Integer.parseInt(value));
                } else if (MoreLikeThisHelperUtil.MLT_MIN_WORD_LENGTH.equals(key)) {
                    mlt.setMinWordLen(Integer.parseInt(value));
                }
            }

            if (text != null) {
                if (FieldNames.PATH.equals(fields[0])) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    TermQuery q = new TermQuery(new Term(FieldNames.PATH, text));
                    TopDocs top = searcher.search(q, 1);
                    if (top.totalHits == 0) {
                        mlt.setFieldNames(fields);
                        moreLikeThisQuery = mlt.like(mlt.getFieldNames()[0], new StringReader(text));
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
                    moreLikeThisQuery = mlt.like(mlt.getFieldNames()[0], new StringReader(text));
                }
            }
            return moreLikeThisQuery;
        } catch (Exception e) {
            throw new RuntimeException("could not handle MLT query " + mltQueryString);
        }
    }
}
