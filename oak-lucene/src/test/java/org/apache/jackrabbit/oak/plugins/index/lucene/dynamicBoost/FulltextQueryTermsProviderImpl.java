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
package org.apache.jackrabbit.oak.plugins.index.lucene.dynamicBoost;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.spi.FulltextQueryTermsProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example fulltext query terms provider.
 */
public class FulltextQueryTermsProviderImpl implements FulltextQueryTermsProvider {

    private static final Logger LOG = LoggerFactory.getLogger(FulltextQueryTermsProviderImpl.class);
    private static final String SEARCH_SPLIT_REGEX = "[ ]";
    private static final String NT_DAM_ASSET = "dam:Asset";
    private static final int MAX_FRAGMENT_SIZE = 2;
    private static final int MAX_QUERY_SIZE = 10;

    private static final String METADATA_FOLDER = "metadata";
    private static final String PREDICTED_TAGS = "predictedTags";
    private static final String PREDICTED_TAGS_REL_PATH = JcrConstants.JCR_CONTENT + "/" + METADATA_FOLDER + "/" + PREDICTED_TAGS + "/";

    @Override
    public Set<String> getSupportedTypes() {
        Set<String> supportedTypes = new HashSet<String>();
        supportedTypes.add(NT_DAM_ASSET);
        return supportedTypes;
    }

    @Override
    public org.apache.lucene.search.Query getQueryTerm(String text, Analyzer analyzer, NodeState indexDefinition) {
        if (analyzer == null || text == null) {
            return null;
        }

        LOG.debug("getQueryTerm Text: {}", text);
        BooleanQuery query = this.createQuery();

        Set<String> charTerms = new HashSet<String>(splitForSearch(text));
        LOG.debug("getQueryTerm charTerms: {}", charTerms);
        if(charTerms.size() > MAX_QUERY_SIZE) {
            LOG.debug("Not adding query terms for smart tags as number of terms in the query {} exceeds " +
                    "maximum permissible value of {}", charTerms.size(), MAX_QUERY_SIZE);
            return null;
        }
        List<String> fragments = prepareFragments(charTerms);

        for(String fragment : fragments) {
            Term term = new Term(PREDICTED_TAGS_REL_PATH + fragment.toLowerCase(), "1");
            query.add(new TermQuery(term), BooleanClause.Occur.SHOULD);
            LOG.debug("Added query term: {}", fragment.toLowerCase());
        }


        Term term = new Term(PREDICTED_TAGS_REL_PATH + text.toLowerCase(), "1");
        query.add(new TermQuery(term), BooleanClause.Occur.SHOULD);
        LOG.debug("Added query term: {}", text.toLowerCase());

        //De-boosting smart tags based query.
        query.setBoost(0.0001f);
        return query;

    }

    private List<String> prepareFragments(Set<String> charTerms) {

        List<String> fragments = new ArrayList<String>();
        Set<Set<String>> powerSet = powerSet(charTerms);

        for(Set<String> set : powerSet) {
            StringBuilder sb = null;
            for(String s : set) {
                if(sb == null) {
                    sb = new StringBuilder();
                }
                sb.append(s);
                if(sb.length() > 0) {
                    sb.append(' ');
                }
            }
            if(sb != null) {
                fragments.add(sb.toString().trim());
            }
        }

        return fragments;
    }

    private <T> Set<Set<T>> powerSet(Set<T> originalSet) {
        Set<Set<T>> powerSet = new HashSet<Set<T>>();
        if (originalSet.isEmpty()) {
            powerSet.add(new HashSet<T>());
            return powerSet;
        }
        List<T> list = new ArrayList<T>(originalSet);
        T head = list.get(0);
        Set<T> rest = new HashSet<T>(list.subList(1, list.size()));
        for (Set<T> subsetExcludingHead : powerSet(rest)) {
            Set<T> subsetIncludingHead = new HashSet<T>();
            subsetIncludingHead.add(head);
            subsetIncludingHead.addAll(subsetExcludingHead);
            if(subsetIncludingHead.size() <= MAX_FRAGMENT_SIZE) {
                powerSet.add(subsetIncludingHead);
            }
            if(subsetExcludingHead.size() <= MAX_FRAGMENT_SIZE) {
                powerSet.add(subsetExcludingHead);
            }
        }
        return powerSet;
    }

    private List<String> splitForSearch(String tagName) {
        return Arrays.asList(removeBackSlashes(tagName).split(SEARCH_SPLIT_REGEX));
    }

    private String removeBackSlashes(String text) {
        return text.replaceAll("\\\\", "");
    }

    protected BooleanQuery createQuery() {
        return new BooleanQuery();
    }

}
