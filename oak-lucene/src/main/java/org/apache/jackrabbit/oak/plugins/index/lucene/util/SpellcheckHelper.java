package org.apache.jackrabbit.oak.plugins.index.lucene.util;

import org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.spell.SuggestWord;

/**
 * Helper class for getting spellcheck results for a given term, calling a {@link org.apache.lucene.search.spell.DirectSpellChecker}
 * under the hood.
 */
public class SpellcheckHelper {
    public static SuggestWord[] getSpellcheck(String spellcheckQueryString, IndexReader reader) {
        DirectSpellChecker spellChecker = new DirectSpellChecker();
        try {
            String text = null;
            for (String param : spellcheckQueryString.split("&")) {
                String[] keyValuePair = param.split("=");
                if (keyValuePair.length != 2 || keyValuePair[0] == null || keyValuePair[1] == null) {
                    throw new RuntimeException("Unparsable native Lucene Spellcheck query: " + spellcheckQueryString);
                } else {
                    if ("term".equals(keyValuePair[0])) {
                        text = keyValuePair[1];
                    }
                }
            }
            if (text != null) {
                return spellChecker.suggestSimilar(new Term(FieldNames.FULLTEXT, text), 10, reader);
            } else {
                return new SuggestWord[0];
            }

        } catch (Exception e) {
            throw new RuntimeException("could not handle Spellcheck query " + spellcheckQueryString);
        }
    }
}
