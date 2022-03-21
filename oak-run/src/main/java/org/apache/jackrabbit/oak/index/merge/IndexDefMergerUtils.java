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
package org.apache.jackrabbit.oak.index.merge;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.IndexName;

/**
 * Utility that allows to merge index definitions.
 */
public class IndexDefMergerUtils {

    private static HashSet<String> IGNORE_LEVEL_0 = new HashSet<>(Arrays.asList(
            "reindex", "refresh", "seed", "reindexCount"));
    private static HashSet<String> USE_PRODUCT_PROPERTY = new HashSet<>(Arrays.asList(
            "jcr:created", "jcr:lastModified", "jcr:uuid", "jcr:createdBy", "jcr:lastModifiedBy", "jcr:createdBy"));
    private static HashSet<String> USE_PRODUCT_CHILD_LEVEL_0 = new HashSet<>(Arrays.asList(
            "tika"));

    /**
     * Merge index definition changes.
     *
     * @param path the path of the change itself (e.g.  /oak:index/lucene-1/indexRules/acme)
     * @param ancestor the common ancestor (the old product index, e.g. lucene)
     * @param customName the name of the node of the customized index (e.g. /oak:index/lucene-1-custom-1)
     * @param custom the latest customized version (e.g. lucene-1-custom-1)
     * @param product the latest product index (e.g. lucene-2)
     * @param productName the name of the node of the latest product index (e.g. /oak:index/lucene-2)
     * @return the merged index definition (e.g. lucene-2-custom-1)
     */
    public static JsonObject merge(String path, JsonObject ancestor, String customName, JsonObject custom, JsonObject product, String productName) {
        ArrayList<String> conflicts = new ArrayList<>();
        JsonObject merged = merge(path, 0, ancestor, custom, product, conflicts);
        if (!conflicts.isEmpty()) {
            throw new UnsupportedOperationException("Conflicts detected: " + conflicts);
        }
        merged.getProperties().put("merges", "[" +
                JsopBuilder.encode(productName) + ", " +
                JsopBuilder.encode(customName) + "]");
        return merged;
    }

    private static JsonObject merge(String path, int level, JsonObject ancestor, JsonObject custom, JsonObject product,
            ArrayList<String> conflicts) {
        Objects.requireNonNull(conflicts);
        return mergeNoNull(path, level,
                        ancestor == null ? new JsonObject() : ancestor,
                        custom == null ? new JsonObject() : custom,
                        product == null ? new JsonObject() : product,
                        conflicts);
    }

    private static JsonObject mergeNoNull(String path, int level, JsonObject ancestor, JsonObject custom, JsonObject product,
            ArrayList<String> conflicts) {
        Objects.requireNonNull(ancestor);
        Objects.requireNonNull(custom);
        Objects.requireNonNull(product);
        Objects.requireNonNull(conflicts);
        JsonObject merged = new JsonObject(true);

        LinkedHashMap<String, Boolean> properties = new LinkedHashMap<>();
        addAllProperties(ancestor, properties);
        addAllProperties(custom, properties);
        addAllProperties(product, properties);

        for (String k : properties.keySet()) {
            if (level == 0 && IGNORE_LEVEL_0.contains(k)) {
                // ignore some properties
                continue;
            }
            if (k.startsWith(":")) {
                // ignore hidden properties
                continue;
            }
            String result = mergeProperty(path, k, ancestor, custom, product, conflicts);
            if (result != null) {
                merged.getProperties().put(k, result);
            }
        }
        LinkedHashMap<String, Boolean> children = new LinkedHashMap<>();
        // first the (new) product index - to ensure the order of children matches the new product index
        addAllChildren(product, children);
        addAllChildren(ancestor, children);
        addAllChildren(custom, children);
        for (String k : children.keySet()) {
            if (k.startsWith(":")) {
                // ignore hidden nodes
                continue;
            }
            JsonObject result = mergeChild(path + "/" + k, k, level, ancestor, custom, product, conflicts);
            if (result != null) {
                merged.getChildren().put(k, result);
            }

        }
        return merged;
    }

    private static void addAllProperties(JsonObject source, LinkedHashMap<String, Boolean> target) {
        for(String k : source.getProperties().keySet()) {
            target.put(k,  true);
        }
    }

    private static void addAllChildren(JsonObject source, LinkedHashMap<String, Boolean> target) {
        for(String k : source.getChildren().keySet()) {
            target.put(k,  true);
        }
    }

    private static String mergeProperty(String path, String property, JsonObject ancestor, JsonObject custom, JsonObject product,
            ArrayList<String> conflicts) {
        if (USE_PRODUCT_PROPERTY.contains(property)) {
            return product.getProperties().get(property);
        }
        String ap = ancestor.getProperties().get(property);
        String cp = custom.getProperties().get(property);
        String pp = product.getProperties().get(property);
        if (Objects.equals(ap, pp) || Objects.equals(cp, pp)) {
            return cp;
        } else if (Objects.equals(ap, cp)) {
            return pp;
        } else {
            conflicts.add("Could not merge value; path=" + path + " property=" + property + "; ancestor=" + ap + "; custom=" + cp
                    + "; product=" + pp);
            return ap;
        }
    }

    private static JsonObject mergeChild(String path, String child, int level, JsonObject ancestor, JsonObject custom, JsonObject product,
            ArrayList<String> conflicts) {
        JsonObject a = ancestor.getChildren().get(child);
        JsonObject c = custom.getChildren().get(child);
        JsonObject p = product.getChildren().get(child);
        if (level == 0 && USE_PRODUCT_CHILD_LEVEL_0.contains(child)) {
            return p;
        }
        if (c == null && p != null) {
            return p; // restore child nodes in product index if removed in custom index
        } else if (isSameJson(a, p) || isSameJson(c, p)) {
            return c;
        } else if (isSameJson(a, c)) {
            return p;
        } else {
            return merge(path, level + 1, a, c, p, conflicts);
        }
    }

    private static boolean isSameJson(JsonObject a, JsonObject b) {
        if (a == null || b == null) {
            return a == null && b == null;
        }
        return a.toString().equals(b.toString());
    }

    /**
     * For indexes that were modified both by the customer and in the product, merge
     * the changes, and create a new index.
     *
     * The new index (if any) is stored in the "newIndexes" object.
     *
     * @param newIndexes the new indexes
     * @param allIndexes all index definitions (including the new ones)
     */
    public static void merge(JsonObject newIndexes, JsonObject allIndexes) {

        // TODO when merging, we keep the product index, so two indexes are created.
        // e.g. lucene, lucene-custom-1, lucene-2, lucene-2-custom-1
        // but if we don't have lucene-2, then we can't merge lucene-3.

        // TODO when merging, e.g. lucene-2-custom-1 is created. but
        // it is only imported in the read-write repo, not in the read-only
        // repository currently. so this new index won't be used;
        // instead, lucene-2 will be used

        List<IndexName> newNames = newIndexes.getChildren().keySet().stream().map(s -> IndexName.parse(s))
                .collect(Collectors.toList());
        Collections.sort(newNames);
        List<IndexName> allNames = allIndexes.getChildren().keySet().stream().map(s -> IndexName.parse(s))
                .collect(Collectors.toList());
        Collections.sort(allNames);
        HashMap<String, JsonObject> mergedMap = new HashMap<>();
        for (IndexName n : newNames) {
            if (n.getCustomerVersion() == 0) {
                IndexName latest = n.getLatestCustomized(allNames);
                IndexName ancestor = n.getLatestProduct(allNames);
                if (latest != null && ancestor != null) {
                    if (n.compareTo(latest) <= 0 || n.compareTo(ancestor) <= 0) {
                        // ignore older versions of indexes
                        continue;
                    }
                    JsonObject latestCustomized = allIndexes.getChildren().get(latest.getNodeName());
                    JsonObject latestAncestor = allIndexes.getChildren().get(ancestor.getNodeName());
                    JsonObject newProduct = newIndexes.getChildren().get(n.getNodeName());
                    try {
                        JsonObject merged = merge(
                                "", latestAncestor,
                                latest.getNodeName(), latestCustomized,
                                newProduct, n.getNodeName());
                        mergedMap.put(n.nextCustomizedName(), merged);
                    } catch (UnsupportedOperationException e) {
                        throw new UnsupportedOperationException("Index: " + n.getNodeName() + ": " + e.getMessage(), e);
                    }
                }
            }
        }
        for (Entry<String, JsonObject> e : mergedMap.entrySet()) {
            newIndexes.getChildren().put(e.getKey(), e.getValue());
        }
    }

}
