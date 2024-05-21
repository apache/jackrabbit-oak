/*
 * COPIED FROM APACHE LUCENE 4.7.2
 *
 * Git URL: git@github.com:apache/lucene.git, tag: releases/lucene-solr/4.7.2, path: lucene/core/src/java
 *
 * (see https://issues.apache.org/jira/browse/OAK-10786 for details)
 */

package org.apache.lucene.search;

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

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.ToStringUtils;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.BasicAutomata;
import org.apache.lucene.util.automaton.BasicOperations;

/**
 * Implements the wildcard search query. Supported wildcards are <code>*</code>, which matches any
 * character sequence (including the empty one), and <code>?</code>, which matches any single
 * character. '\' is the escape character.
 * <p>
 * Note this query can be slow, as it needs to iterate over many terms. In order to prevent
 * extremely slow WildcardQueries, a Wildcard term should not start with the wildcard
 * <code>*</code>
 *
 * <p>This query uses the {@link
 * MultiTermQuery#CONSTANT_SCORE_AUTO_REWRITE_DEFAULT} rewrite method.
 *
 * @see AutomatonQuery
 */
public class WildcardQuery extends AutomatonQuery {

    /**
     * String equality with support for wildcards
     */
    public static final char WILDCARD_STRING = '*';

    /**
     * Char equality with support for wildcards
     */
    public static final char WILDCARD_CHAR = '?';

    /**
     * Escape character
     */
    public static final char WILDCARD_ESCAPE = '\\';

    /**
     * Constructs a query for terms matching <code>term</code>.
     */
    public WildcardQuery(Term term) {
        super(term, toAutomaton(term));
    }

    /**
     * Convert Lucene wildcard syntax into an automaton.
     *
     * @lucene.internal
     */
    @SuppressWarnings("fallthrough")
    public static Automaton toAutomaton(Term wildcardquery) {
        List<Automaton> automata = new ArrayList<Automaton>();

        String wildcardText = wildcardquery.text();

        for (int i = 0; i < wildcardText.length(); ) {
            final int c = wildcardText.codePointAt(i);
            int length = Character.charCount(c);
            switch (c) {
                case WILDCARD_STRING:
                    automata.add(BasicAutomata.makeAnyString());
                    break;
                case WILDCARD_CHAR:
                    automata.add(BasicAutomata.makeAnyChar());
                    break;
                case WILDCARD_ESCAPE:
                    // add the next codepoint instead, if it exists
                    if (i + length < wildcardText.length()) {
                        final int nextChar = wildcardText.codePointAt(i + length);
                        length += Character.charCount(nextChar);
                        automata.add(BasicAutomata.makeChar(nextChar));
                        break;
                    } // else fallthru, lenient parsing with a trailing \
                default:
                    automata.add(BasicAutomata.makeChar(c));
            }
            i += length;
        }

        return BasicOperations.concatenate(automata);
    }

    /**
     * Returns the pattern term.
     */
    public Term getTerm() {
        return term;
    }

    /**
     * Prints a user-readable version of this query.
     */
    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        if (!getField().equals(field)) {
            buffer.append(getField());
            buffer.append(":");
        }
        buffer.append(term.text());
        buffer.append(ToStringUtils.boost(getBoost()));
        return buffer.toString();
    }
}
