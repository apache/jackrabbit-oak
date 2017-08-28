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

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames;
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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
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

    public MTFulltextQueryTermsProvider(Decoder decoder, Set<String> nodeTypes, float minScore) {
        this.decoder = decoder;
        this.nodeTypes = nodeTypes;
        this.minScore = minScore;
    }

    @Override
    public Query getQueryTerm(String text, Analyzer analyzer, NodeState indexDefinition) {
        BooleanQuery query = new BooleanQuery();
        Sentence sentence = new Sentence(text, 0, decoder.getJoshuaConfiguration());
        Translation translation = decoder.decode(sentence);
        log.debug("{} decoded into {}", text, translation);
        // try phrase translation first
        List<StructuredTranslation> structuredTranslations = translation.getStructuredTranslations();
        if (!structuredTranslations.isEmpty()) {
            addTranslations(query, structuredTranslations);
        } else {
            // if phrase cannot be translated, perform token by token translation
            try {
                TokenStream tokenStream = analyzer.tokenStream(null, new StringReader(text));
                tokenStream.addAttribute(CharTermAttribute.class);
                tokenStream.reset();
                while (tokenStream.incrementToken()) {
                    CharTermAttribute attribute = tokenStream.getAttribute(CharTermAttribute.class);
                    Translation translatedToken = decoder.decode(new Sentence(attribute.toString(), 0,
                            decoder.getJoshuaConfiguration()));
                    addTranslations(query, translatedToken.getStructuredTranslations());
                }
                tokenStream.end();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }
        return query.clauses().size() > 0 ? query : null;
    }

    private void addTranslations(BooleanQuery query, List<StructuredTranslation> structuredTranslations) {
        for (StructuredTranslation st : structuredTranslations) {
            String translationString = st.getTranslationString();
            if (st.getTranslationScore() > minScore) {
                query.add(new BooleanClause(new TermQuery(new Term(FieldNames.FULLTEXT, translationString)),
                        BooleanClause.Occur.SHOULD));
                log.debug("added query for translated phrase {}", translationString);
                List<String> translationTokens = st.getTranslationTokens();
                int i = 0;
                // if output is a phrase, look for tokens having a word alignment to the original sentence terms
                for (List<Integer> wa : st.getTranslationWordAlignments()) {
                    if (!wa.isEmpty()) {
                        String translatedTerm = translationTokens.get(i);
                        log.debug("added query for translated token {}", translatedTerm);
                        query.add(new BooleanClause(new TermQuery(new Term(FieldNames.FULLTEXT, translatedTerm)),
                                BooleanClause.Occur.SHOULD));
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
