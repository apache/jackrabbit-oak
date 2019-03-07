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
import java.util.*;

import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
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
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;

/**
 * Utility methods for indexing and searching for similar feature vectors
 */
public class SimSearchUtils {

    private static final Logger log = LoggerFactory.getLogger(SimSearchUtils.class);

    public static String toDoubleString(byte[] bytes) {
        double[] a = toDoubleArray(bytes);
        StringBuilder builder = new StringBuilder();
        for (Double d : a) {
            if (builder.length() > 0) {
                builder.append(' ');
            }
            builder.append(d);
        }
        return builder.toString();
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

    private static double[] toDoubleArray(byte[] array) {
        int blockSize = Double.SIZE / Byte.SIZE;
        ByteBuffer wrap = ByteBuffer.wrap(array);
        int capacity = array.length / blockSize;
        double[] doubles = new double[capacity];
        for (int i = 0; i < capacity; i++) {
                double e = wrap.getDouble(i * blockSize);
                doubles[i] = e;
            }
        return doubles;
    }

    private static Collection<BytesRef> getTokens(Analyzer analyzer, String field, String sampleTextString) throws IOException {
        Collection<BytesRef> tokens = new LinkedList<>();
        TokenStream ts = analyzer.tokenStream(field, sampleTextString);
        ts.addAttribute(CharTermAttribute.class);
        ts.reset();
        while (ts.incrementToken()) {
            CharTermAttribute charTermAttribute = ts.getAttribute(CharTermAttribute.class);
            String token = new String(charTermAttribute.buffer(), 0, charTermAttribute.length());
            tokens.add(new BytesRef(token));
        }
        ts.end();
        ts.close();
        return tokens;
    }

    static Query getSimQuery(Analyzer analyzer, String fieldName, String text) throws IOException {
        return createLSHQuery(fieldName, getTokens(analyzer, fieldName, text), 1f,1f);
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
                            Query simQuery = SimSearchUtils.getSimQuery(analyzer, similarityFieldName, fvString);
                            booleanQuery.add(new BooleanClause(simQuery, SHOULD));
                            String[] binaryTags = doc.getValues(FieldNames.SIMILARITY_TAGS);
                            if (binaryTags != null && binaryTags.length > 0) {
                                BooleanQuery tagQuery = new BooleanQuery();
                                for (String brt : binaryTags) {
                                    tagQuery.add(new BooleanClause(new TermQuery(new Term(FieldNames.SIMILARITY_TAGS, brt)), SHOULD));
                                }
                                tagQuery.setBoost(0.5f);
                                booleanQuery.add(tagQuery, SHOULD);
                            }
                            log.trace("similarity query generated for {}", pd.name);
                        } else {
                            log.warn("could not create query for similarity field {}", similarityFieldName);
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

    private static Query createLSHQuery(String field, Collection<BytesRef> minhashes,
                                        float similarity, float expectedTruePositive) {
        int bandSize = 1;
        if (expectedTruePositive < 1) {
            bandSize = computeBandSize(minhashes.size(), similarity, expectedTruePositive);
        }

        BooleanQuery builder = new BooleanQuery();
        BooleanQuery childBuilder = new BooleanQuery();
        int rowInBand = 0;
        for (BytesRef minHash : minhashes) {
            TermQuery tq = new TermQuery(new Term(field, minHash));
            if (bandSize == 1) {
                builder.add(new ConstantScoreQuery(tq), BooleanClause.Occur.SHOULD);
            } else {
                childBuilder.add(new ConstantScoreQuery(tq), BooleanClause.Occur.MUST);
                rowInBand++;
                if (rowInBand == bandSize) {
                    builder.add(new ConstantScoreQuery(childBuilder),
                            BooleanClause.Occur.SHOULD);
                    childBuilder = new BooleanQuery();
                    rowInBand = 0;
                }
            }
        }
        // Avoid a dubious narrow band, wrap around and pad with the start
        if (childBuilder.clauses().size() > 0) {
            for (BytesRef token : minhashes) {
                TermQuery tq = new TermQuery(new Term(field, token.toString()));
                childBuilder.add(new ConstantScoreQuery(tq), BooleanClause.Occur.MUST);
                rowInBand++;
                if (rowInBand == bandSize) {
                    builder.add(new ConstantScoreQuery(childBuilder),
                            BooleanClause.Occur.SHOULD);
                    break;
                }
            }
        }

        if (expectedTruePositive >= 1.0 && similarity < 1) {
            builder.setMinimumNumberShouldMatch((int) (Math.ceil(minhashes.size() * similarity)));
        }
        log.trace("similarity query with bands : {}, minShouldMatch : {}, no. of clauses : {}", bandSize,
                builder.getMinimumNumberShouldMatch(), builder.clauses().size());
        return builder;

    }

    private static int computeBandSize(int numHash, double similarity, double expectedTruePositive) {
        for (int bands = 1; bands <= numHash; bands++) {
            int rowsInBand = numHash / bands;
            double truePositive = 1 - Math.pow(1 - Math.pow(similarity, rowsInBand), bands);
            if (truePositive > expectedTruePositive) {
                return rowsInBand;
            }
        }
        return 1;
    }

    public static void bruteForceFVRerank(List<PropertyDefinition> sp, TopDocs docs, IndexSearcher indexSearcher) throws IOException {
        double distSum = 0d;
        double counter = 0d;
        Map<Integer, Double> distances = new HashMap<>();
        int k = 15;
        ScoreDoc inputDoc = docs.scoreDocs[0]; // we assume the input doc is the first one returned
        List<Integer> toDiscard = new LinkedList<>();
        for (PropertyDefinition pd : sp) {
            String fieldName = FieldNames.createBinSimilarityFieldName(pd.name);
            BytesRef binaryValue = indexSearcher.doc(inputDoc.doc).getBinaryValue(fieldName);
            if (binaryValue != null) {
                double[] inputVector = toDoubleArray(binaryValue.bytes);
                for (int j = 0; j < docs.scoreDocs.length; j++) {
                    BytesRef featureVectorBinary = indexSearcher.doc(docs.scoreDocs[j].doc)
                            .getBinaryValue(fieldName);
                    if (featureVectorBinary != null) {
                        double[] currentVector = toDoubleArray(featureVectorBinary.bytes);
                        double distance = dist(inputVector, currentVector) + 1e-10; // constant term to avoid division by zero
                        if (Double.isNaN(distance) || Double.isInfinite(distance)) {
                            toDiscard.add(docs.scoreDocs[j].doc);
                        } else {
                            distSum += distance;
                            counter++;
                            distances.put(docs.scoreDocs[j].doc, distance);
                            docs.scoreDocs[j].score = (float) (1d / distance);
                        }
                    }
                }
            }
        }

        // remove docs having invalid distance
        if (!toDiscard.isEmpty()) {
            docs.scoreDocs = Arrays.stream(docs.scoreDocs).filter(e -> !toDiscard.contains(e.doc)).toArray(ScoreDoc[]::new);
        }

        // remove docs whose distance is one order of magnitude higher than average distance
        final double distanceThreshold = 10 * distSum / counter;
        docs.scoreDocs = Arrays.stream(docs.scoreDocs).filter(e -> distances.containsKey(e.doc) && distances.get(e.doc) < distanceThreshold).toArray(ScoreDoc[]::new);

        // rerank scoreDocs
        Arrays.parallelSort(docs.scoreDocs, 0, docs.scoreDocs.length, (o1, o2) -> {
            return -1 * Double.compare(o1.score, o2.score);
        });

        // retain only the top k nearest neighbours
        if (docs.scoreDocs.length > k) {
            docs.scoreDocs = Arrays.copyOfRange(docs.scoreDocs, 0, k);
        }

        if (docs.scoreDocs.length > 0) {
            docs.setMaxScore(docs.scoreDocs[0].score);
        }
    }

    private static double dist(double[] x, double[] y) { // euclidean distance
        double d = 0;
        for (int i = 0; i < x.length; i++) {
            d += Math.pow(y[i] - x[i], 2);
        }
        return Math.sqrt(d);
    }

}