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
package org.apache.jackrabbit.oak.plugins.index.mongot.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongoFieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.bson.Document;

final class MongotResultAdapter {

    static final String HIGHLIGHTS = "_highlights";

    private MongotResultAdapter() {
    }

    static Map<String, String> excerpts(Document result, List<String> requestedColumns) {
        Map<String, List<String>> valuesByPath = new HashMap<>();
        List<Document> highlights = result.getList(HIGHLIGHTS, Document.class, List.of());
        for (Document highlight : highlights) {
            String path = highlight.getString("path");
            List<Document> fragments = highlight.getList("texts", Document.class, List.of());
            StringBuilder value = new StringBuilder();
            for (Document fragment : fragments) {
                String text = fragment.getString("value");
                if ("hit".equals(fragment.getString("type"))) {
                    value.append("<strong>").append(text).append("</strong>");
                } else {
                    value.append(text);
                }
            }
            valuesByPath.computeIfAbsent(path, ignored -> new ArrayList<>()).add(value.toString());
        }

        Map<String, String> excerpts = new LinkedHashMap<>();
        for (String column : requestedColumns) {
            List<String> values = valuesByPath.get(highlightPath(column));
            if (values != null && !values.isEmpty()) {
                excerpts.put(column, values.get(0));
            }
        }
        return excerpts;
    }

    static List<String> highlightPaths(List<String> requestedColumns) {
        return requestedColumns.stream()
                .map(MongotResultAdapter::highlightPath)
                .distinct()
                .toList();
    }

    static FulltextIndex.FacetProvider facets(List<Document> results) {
        Map<String, Map<String, Integer>> countsByField = new HashMap<>();
        for (Document result : results) {
            Document facetValues = result.get(MongoFieldNames.FACET, Document.class);
            if (facetValues == null) {
                continue;
            }
            for (Map.Entry<String, Object> field : facetValues.entrySet()) {
                Set<String> labels = new LinkedHashSet<>();
                if (field.getValue() instanceof Collection<?>) {
                    for (Object value : (Collection<?>) field.getValue()) {
                        labels.add(String.valueOf(value));
                    }
                } else if (field.getValue() != null) {
                    labels.add(String.valueOf(field.getValue()));
                }
                Map<String, Integer> counts = countsByField.computeIfAbsent(
                        field.getKey(), ignored -> new HashMap<>());
                labels.forEach(label -> counts.merge(label, 1, Integer::sum));
            }
        }

        return (numberOfFacets, columnName) -> {
            String property = FulltextIndex.parseFacetField(columnName);
            Map<String, Integer> counts = countsByField.getOrDefault(
                    MongoFieldNames.encodeProperty(property), Map.of());
            return counts.entrySet().stream()
                    .sorted(Map.Entry.<String, Integer>comparingByValue(Comparator.reverseOrder())
                            .thenComparing(Map.Entry.comparingByKey()))
                    .limit(Math.max(0, numberOfFacets))
                    .map(entry -> new FulltextIndex.Facet(entry.getKey(), entry.getValue()))
                    .toList();
        };
    }

    static List<FulltextIndex.FulltextResultRow> suggestions(List<Document> results, String term) {
        Map<String, Double> bestScoreByValue = new HashMap<>();
        for (Document result : results) {
            Number score = result.get("_score", Number.class);
            double valueScore = score == null ? 1.0d : score.doubleValue();
            for (String value : result.getList(MongoFieldNames.SUGGEST, String.class, List.of())) {
                if (matchesSuggestion(value, term)) {
                    bestScoreByValue.merge(value, valueScore, Math::max);
                }
            }
        }
        return bestScoreByValue.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue(Comparator.reverseOrder())
                        .thenComparing(Map.Entry.comparingByKey()))
                .map(entry -> new FulltextIndex.FulltextResultRow(
                        entry.getKey(), entry.getValue()))
                .toList();
    }

    static List<FulltextIndex.FulltextResultRow> spellchecks(List<Document> results, String term) {
        Map<String, Integer> frequencies = new HashMap<>();
        for (Document result : results) {
            Set<String> documentCandidates = new LinkedHashSet<>();
            for (String value : result.getList(MongoFieldNames.SPELLCHECK, String.class, List.of())) {
                for (String token : value.toLowerCase(Locale.ROOT).split("[^\\p{L}\\p{N}]+")) {
                    if (!token.isEmpty()) {
                        documentCandidates.add(token);
                    }
                }
            }
            documentCandidates.forEach(candidate -> frequencies.merge(candidate, 1, Integer::sum));
        }
        String[] terms = term.toLowerCase(Locale.ROOT).trim().split("\\s+");
        if (terms.length == 1) {
            return correctionCandidates(terms[0], frequencies, false).stream()
                    .limit(10)
                    .map(value -> new FulltextIndex.FulltextResultRow(value, frequencies.get(value)))
                    .toList();
        }

        List<String> corrected = new ArrayList<>();
        int score = 0;
        boolean changed = false;
        for (String input : terms) {
            List<String> candidates = correctionCandidates(input, frequencies, true);
            if (candidates.isEmpty()) {
                return List.of();
            }
            String candidate = candidates.get(0);
            corrected.add(candidate);
            score += frequencies.get(candidate);
            changed |= !candidate.equals(input);
        }
        return changed
                ? List.of(new FulltextIndex.FulltextResultRow(String.join(" ", corrected), score))
                : List.of();
    }

    private static List<String> correctionCandidates(String input,
                                                     Map<String, Integer> frequencies,
                                                     boolean includeExact) {
        int maxEdits = input.length() <= 4 ? 1 : 2;
        return frequencies.keySet().stream()
                .filter(value -> {
                    int distance = editDistance(input, value);
                    return distance <= maxEdits && (includeExact || distance > 0);
                })
                .sorted(Comparator.comparingInt((String value) -> editDistance(input, value))
                        .thenComparing(Comparator.comparingInt(
                                (String value) -> frequencies.get(value)).reversed())
                        .thenComparing(value -> value))
                .toList();
    }

    private static boolean matchesSuggestion(String value, String term) {
        String normalizedValue = value.toLowerCase(Locale.ROOT);
        String normalizedTerm = term.toLowerCase(Locale.ROOT).trim();
        if (normalizedTerm.indexOf(' ') >= 0) {
            return normalizedValue.contains(normalizedTerm);
        }
        for (String token : normalizedValue.split("[^\\p{L}\\p{N}]+")) {
            if (token.startsWith(normalizedTerm)) {
                return true;
            }
        }
        return false;
    }

    private static int editDistance(String left, String right) {
        int[] previous = new int[right.length() + 1];
        for (int j = 0; j <= right.length(); j++) {
            previous[j] = j;
        }
        for (int i = 1; i <= left.length(); i++) {
            int[] current = new int[right.length() + 1];
            current[0] = i;
            for (int j = 1; j <= right.length(); j++) {
                int substitution = previous[j - 1]
                        + (left.charAt(i - 1) == right.charAt(j - 1) ? 0 : 1);
                current[j] = Math.min(Math.min(previous[j] + 1, current[j - 1] + 1),
                        substitution);
            }
            previous = current;
        }
        return previous[right.length()];
    }

    private static String highlightPath(String column) {
        if (QueryConstants.REP_EXCERPT.equals(column)
                || "rep:excerpt()".equals(column)
                || "rep:excerpt(.)".equals(column)) {
            return MongoFieldNames.FULLTEXT;
        }
        String property = column.substring(QueryConstants.REP_EXCERPT.length() + 1,
                column.length() - 1);
        return MongoFieldNames.ANALYZED + "." + MongoFieldNames.encodeProperty(property);
    }
}
