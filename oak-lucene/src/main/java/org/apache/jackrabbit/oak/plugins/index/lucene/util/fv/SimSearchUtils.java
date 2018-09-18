/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.lucene.util.fv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.lucene.PropertyDefinition;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;

/**
 * Utility methods for indexing and searching for similar feature vectors
 */
public class SimSearchUtils {

    private static final Logger log = LoggerFactory.getLogger(SimSearchUtils.class);

    public static String toDoubleString(byte[] bytes) {
        Double[] a = toDoubleArray(bytes);
        StringBuilder builder = new StringBuilder();
        for (Double d : a) {
            if (builder.length() > 0) {
                builder.append(' ');
            }
            builder.append(d);
        }
        return builder.toString();
    }

    private static Double[] toDoubleArray(byte[] array) {
        List<Double> doubles = toDoubles(array);
        return doubles.toArray(new Double[doubles.size()]);
    }

    public static List<Double> toDoubles(byte[] array) {
        int blockSize = Double.SIZE / Byte.SIZE;
        ByteBuffer wrap = ByteBuffer.wrap(array);
        int capacity = array.length / blockSize;
        List<Double> doubles = new ArrayList<>(capacity);
        for (int i = 0; i < capacity; i++) {
            double e = wrap.getDouble(i * blockSize);
            doubles.add(e);
        }
        return doubles;
    }

    private static Collection<String> getTokens(Analyzer analyzer, String field, String sampleTextString) throws IOException {
        Collection<String> tokens = new LinkedList<>();
        TokenStream ts = analyzer.tokenStream(field, sampleTextString);
        ts.reset();
        ts.addAttribute(CharTermAttribute.class);
        while (ts.incrementToken()) {
            CharTermAttribute charTermAttribute = ts.getAttribute(CharTermAttribute.class);
            String token = new String(charTermAttribute.buffer(), 0, charTermAttribute.length());
            tokens.add(token);
        }
        ts.end();
        ts.close();
        return tokens;
    }

    static BooleanQuery getSimQuery(Analyzer analyzer, String fieldName, String text) throws IOException {
        Collection<String> tokens = getTokens(analyzer, fieldName, text);
        BooleanQuery booleanQuery = new BooleanQuery(true);
        booleanQuery.setMinimumNumberShouldMatch(3);
        for (String token : tokens) {
            booleanQuery.add(new ConstantScoreQuery(new TermQuery(new Term(fieldName, token))), BooleanClause.Occur.SHOULD);
        }
        return booleanQuery;
    }


    public static byte[] toByteArray(List<Double> values) {
        int blockSize = Double.SIZE / Byte.SIZE;
        byte[] bytes = new byte[values.size() * blockSize];
        for (int i = 0, j = 0; i < values.size(); i++, j += blockSize) {
            ByteBuffer.wrap(bytes, j, blockSize).putDouble(values.get(i));
        }
        return bytes;
    }

    public static byte[] toByteArray(String value) {
        List<Double> doubles = new LinkedList<>();
        for (String dv : value.split(",")) {
            doubles.add(Double.parseDouble(dv));
        }
        return toByteArray(doubles);
    }

    public static Query getSimilarityQuery(List<PropertyDefinition> sp, IndexReader reader, String queryString) {
        try {
            log.debug("parsing similarity query on {}", queryString);
            Query similarityQuery = null;
            String text = null;
            for (String param : queryString.split("&")) {
                String[] keyValuePair = param.split("=");
                if (keyValuePair.length != 2 || keyValuePair[0] == null || keyValuePair[1] == null) {
                    throw new RuntimeException("Unparsable native Lucene query for fv similarity: " + queryString);
                } else {
                    if ("stream.body".equals(keyValuePair[0])) {
                        text = keyValuePair[1];
                        break;
                    }
                }
            }

            if (text != null && !sp.isEmpty()) {
                log.debug("generating similarity query for {}", text);
                BooleanQuery booleanQuery = new BooleanQuery(true);
                LSHAnalyzer analyzer = new LSHAnalyzer();
                IndexSearcher searcher = new IndexSearcher(reader);
                TermQuery q = new TermQuery(new Term(FieldNames.PATH, text));
                TopDocs top = searcher.search(q, 1);
                if (top.totalHits > 0) {
                    ScoreDoc d = top.scoreDocs[0];
                    Document doc = reader.document(d.doc);
                    for (PropertyDefinition pd : sp) {
                        log.debug("adding similarity clause for property {}", pd.name);
                        String similarityFieldName = FieldNames.createSimilarityFieldName(pd.name);
                        String fvString = doc.get(similarityFieldName);
                        if (fvString != null && fvString.trim().length() > 0) {
                            log.trace("generating sim query on field {} and text {}", similarityFieldName, fvString);
                            BooleanQuery simQuery = SimSearchUtils.getSimQuery(analyzer, similarityFieldName, fvString);
                            booleanQuery.add(new BooleanClause(simQuery, SHOULD));
                            log.trace("similarity query generated for {}", pd.name);
                        }
                    }
                }
                if (booleanQuery.clauses().size() > 0) {
                    similarityQuery = booleanQuery;
                    log.trace("final similarity query is {}", similarityQuery);
                }
            }

            return similarityQuery;
        } catch (Exception e) {
            throw new RuntimeException("could not handle similarity query " + queryString);
        }
    }

}
