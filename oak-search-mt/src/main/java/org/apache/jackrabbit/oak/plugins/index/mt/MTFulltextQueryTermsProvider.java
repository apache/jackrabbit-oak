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
package org.apache.jackrabbit.oak.plugins.index.mt;

import java.io.StringReader;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.lucene.OakAnalyzer;
import org.apache.jackrabbit.oak.plugins.index.lucene.spi.FulltextQueryTermsProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.joshua.decoder.Decoder;
import org.apache.joshua.decoder.StructuredTranslation;
import org.apache.joshua.decoder.Translation;
import org.apache.joshua.decoder.segment_file.Sentence;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.simple.SimpleQueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link FulltextQueryTermsProvider} that performs machine translation on full text returning a query containing
 * translated tokens.
 */
public class MTFulltextQueryTermsProvider implements FulltextQueryTermsProvider {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Decoder decoder;
    private final Set<String> nodeTypes;
    private final float minScore;
    private final SimpleQueryParser qp;

    public MTFulltextQueryTermsProvider(Decoder decoder, Set<String> nodeTypes, float minScore) {
        this.decoder = decoder;
        this.nodeTypes = nodeTypes;
        this.minScore = minScore;
        this.qp = new SimpleQueryParser(new OakAnalyzer(Version.LUCENE_47), FieldNames.FULLTEXT);
    }

    @Override
    public Query getQueryTerm(String text, Analyzer analyzer, NodeState indexDefinition) {

        BooleanQuery query = new BooleanQuery();
        try {
            Sentence sentence = new Sentence(text, text.hashCode(), decoder.getJoshuaConfiguration());
            Translation translation = decoder.decode(sentence);
            log.debug("{} decoded into {}", text, translation);
            query.add(new BooleanClause(new TermQuery(new Term(FieldNames.FULLTEXT, translation.toString())), BooleanClause.Occur.SHOULD));


            // try phrase translation first
            List<StructuredTranslation> structuredTranslations = translation.getStructuredTranslations();
            log.debug("found {} structured translations", structuredTranslations.size());
            if (!structuredTranslations.isEmpty()) {
                log.debug("phrase translation");
                addTranslations(query, structuredTranslations);
            } else {
                // if phrase cannot be translated, perform token by token translation
                log.debug("per token translation");

                TokenStream tokenStream = analyzer.tokenStream(null, new StringReader(text));
                tokenStream.addAttribute(CharTermAttribute.class);
                tokenStream.reset();
                while (tokenStream.incrementToken()) {
                    CharTermAttribute attribute = tokenStream.getAttribute(CharTermAttribute.class);
                    String source = attribute.toString();
                    Translation translatedToken = decoder.decode(new Sentence(source, source.hashCode(),
                            decoder.getJoshuaConfiguration()));
                    addTranslations(query, translatedToken.getStructuredTranslations());
                }
                tokenStream.end();
            }

        } catch (Exception e) {
            log.error("could not translate query", e);
        }
        return query.clauses().size() > 0 ? query : null;
    }

    private void addTranslations(BooleanQuery query, List<StructuredTranslation> structuredTranslations) {
        for (StructuredTranslation st : structuredTranslations) {
            String translationString = st.getTranslationString();
            float translationScore = st.getTranslationScore();
            log.debug("translation {} has score {}", translationString, translationScore);
            if (translationScore > minScore) {
                log.debug("translation score for {} is {}", translationString, translationScore);
                query.add(new BooleanClause(qp.createPhraseQuery(FieldNames.FULLTEXT, translationString),
                        BooleanClause.Occur.SHOULD));
                log.debug("added query for translated phrase {}", translationString);
                List<String> translationTokens = st.getTranslationTokens();
                int i = 0;
                // if output is a phrase, look for tokens having a word alignment to the original sentence terms
                for (List<Integer> wa : st.getTranslationWordAlignments()) {
                    if (!wa.isEmpty()) {
                        String translatedTerm = translationTokens.get(i);
                        Query termQuery = qp.parse(translatedTerm);
                        query.add(new BooleanClause(termQuery, BooleanClause.Occur.SHOULD));
                        log.debug("added query for translated token {}", translatedTerm);
                    }
                    i++;
                }
            }
        }
    }

    public void clearResources() {
        decoder.cleanUp();
    }

    @Nonnull
    @Override
    public Set<String> getSupportedTypes() {
        return nodeTypes;
    }
}
