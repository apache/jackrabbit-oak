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
package org.apache.jackrabbit.oak.plugins.index.diff;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.json.Base64BlobSerializer;
import org.apache.jackrabbit.oak.json.JsonSerializer;
import org.apache.jackrabbit.oak.plugins.index.IndexName;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Index definition merge utility that uses the "diff" mode.
 */
public class DiffIndexMerger {

    private static final Logger LOG = LoggerFactory.getLogger(DiffIndexMerger.class);

    public static final String DIFF_INDEX = "diff.index";
    public static final String DIFF_INDEX_OPTIMIZER = "diff.index.optimizer";

    public static final String LAST_PROCESSED = ":lastProcessed";
    public static final String MERGE_CHECKSUM = "mergeChecksum";

    private static final String MERGE_INFO = "This index was auto-merged. See also https://oak-indexing.github.io/oakTools/simplified.html";

    // the list of unsupported included paths, e.g. "/apps,/libs"
    // by default all paths are supported
    private static final String[] UNSUPPORTED_INCLUDED_PATHS = System.getProperty(
            "oak.diffIndex.unsupportedPaths", "").split(",");

    // in case a custom index is removed, whether a dummy index is created
    private static final boolean DELETE_CREATES_DUMMY = Boolean.getBoolean(
            "oak.diffIndex.deleteCreatesDummy");

    // in case a customization was removed, create a copy of the OOTB index
    private static final boolean DELETE_COPIES_OOTB = Boolean.getBoolean(
            "oak.diffIndex.deleteCopiesOOTB");

    // whether to log at info level
    private static final boolean LOG_AT_INFO_LEVEL = Boolean.getBoolean(
            "oak.diffIndex.logAtInfoLevel");

    // the set of top-level properties that is not allowed to be added to an existing index
    private static final Set<String> REJECTED_TOP_LEVEL_PROPS_FOR_EXISTING_INDEX = Set.of(
            "selectionPolicy", "valueRegex", "queryFilterRegex", "includedPaths", "excludedPaths", "queryPaths");

    // the set of child properties that is not allowed to be added if the property is already indexed
    // eg. the "name" property may not need to be set if the existing property doesn't have it yet (eg. a function-based index),
    // or the "function" property may not need to be set unless if it already exists (eg. a name-based index)
    private static final Set<String> REJECTED_ADDING_TO_EXISTING_PROPERTY = Set.of(
            "isRegexp", "index", "function", "name");

    // set of properties that are allowed to be changed if the property already exists
    private static final Set<String> ALLOW_CHANGING_IN_EXISTING_PROPERTY = Set.of(
            "boost", "weight");

    // set of properties that allow multi-valued string that might be merged
    private static final Set<String> MERGE_MULTI_VALUES_STRINGS = Set.of(
            "includedPaths", "queryPaths", "tags");

    // maximum number of warnings to keep
    private static final int MAX_WARNINGS = 100;

    // maximum total size of warnings (1 MB)
    private static final int MAX_WARNINGS_SIZE = 1024 * 1024;

    private String[] unsupportedIncludedPaths;
    private boolean deleteCreatesDummyIndex;
    private boolean deleteCopiesOutOfTheBoxIndex;
    private boolean logAtInfoLevel;

    // thread-safe queue to store warnings (oldest first)
    private final LinkedList<String> warnings = new LinkedList<>();
    private int warningsSize = 0;

    public DiffIndexMerger() {
        this(UNSUPPORTED_INCLUDED_PATHS, DELETE_CREATES_DUMMY, DELETE_COPIES_OOTB, LOG_AT_INFO_LEVEL);
    }

    DiffIndexMerger(String[] unsupportedIncludedPaths,
            boolean deleteCreatesDummyIndex, boolean deleteCopiesOutOfTheBoxIndex,
            boolean logAtInfoLevel) {
        this.unsupportedIncludedPaths = unsupportedIncludedPaths;
        this.deleteCreatesDummyIndex = deleteCreatesDummyIndex;
        this.deleteCopiesOutOfTheBoxIndex = deleteCopiesOutOfTheBoxIndex;
        this.logAtInfoLevel = logAtInfoLevel;
    }

    /**
     * If there is a diff index, that is an index with prefix "diff.", then try to merge it.
     *
     * @param newImageLuceneDefinitions
     *        the new indexes
     *        (input and output)
     * @param repositoryDefinitions
     *        the indexes in the writable repository
     *        (input)
     * @param repositoryNodeStore
     */
    public void merge(JsonObject newImageLuceneDefinitions, JsonObject repositoryDefinitions, NodeStore repositoryNodeStore) {
        // combine all definitions into one object
        JsonObject combined = new JsonObject();

        // index definitions in the repository
        combined.getChildren().putAll(repositoryDefinitions.getChildren());

        // read the diff.index.optimizer explicitly,
        // because it's a not a regular index definition,
        // and so it is not in the repositoryDefinitions
        if (repositoryNodeStore != null) {
            Map<String, JsonObject> diffInRepo = readDiffIndex(repositoryNodeStore, DIFF_INDEX_OPTIMIZER);
            combined.getChildren().putAll(diffInRepo);
        }

        // overwrite with the provided definitions (if any)
        combined.getChildren().putAll(newImageLuceneDefinitions.getChildren());

        // check if there "diff.index" or "diff.index.optimizer"
        boolean found = combined.getChildren().containsKey("/oak:index/" + DIFF_INDEX)
                || combined.getChildren().containsKey("/oak:index/" + DIFF_INDEX_OPTIMIZER);
        if (!found) {
            // early exit, so that the risk of merging the PR
            // is very small for customers that do not use this
            log("No 'diff.index' definition");
            return;
        }
        mergeDiff(newImageLuceneDefinitions, combined);
    }

    /**
     * If there is a diff index (hardcoded node "/oak:index/diff.index" or
     * "/oak:index/diff.index.optimizer"), then iterate over all entries and create new
     * (merged) versions if needed.
     *
     * @param newImageLuceneDefinitions
     *        the new Lucene definitions
     *        (input + output)
     * @param combined
     *        the definitions in the repository,
     *        including the one in the customer repo and new ones
     *        (input)
     * @return whether a new version of an index was added
     */
    boolean mergeDiff(JsonObject newImageLuceneDefinitions, JsonObject combined) {
        // iterate again, this time process

        // collect the diff index(es)
        HashMap<String, JsonObject> toProcess = new HashMap<>();
        tryExtractDiffIndex(combined, "/oak:index/" + DIFF_INDEX, toProcess);
        tryExtractDiffIndex(combined, "/oak:index/" + DIFF_INDEX_OPTIMIZER, toProcess);
        // if the diff index exists, but doesn't contain some of the previous indexes
        // (indexes with mergeInfo), then we need to disable those (using /dummy includedPath)
        extractExistingMergedIndexes(combined, toProcess);
        if (toProcess.isEmpty()) {
            log("No diff index definitions found.");
            return false;
        }
        boolean hasChanges = false;
        for (Entry<String, JsonObject> e : toProcess.entrySet()) {
            String key = e.getKey();
            JsonObject value = e.getValue();
            if (key.startsWith("/oak:index/")) {
                logAndCollectWarn("The key should contain just the index name, without the '/oak:index' prefix, for key: {}", key);
                key = key.substring("/oak:index/".length());
            }
            log("Processing {}", key);
            hasChanges |= processMerge(key, value, newImageLuceneDefinitions, combined);
        }
        return hasChanges;
    }

    /**
     * Extract a "diff.index" from the set of index definitions (if found), and if
     * found, store the nested entries in the target map, merging them with previous
     * entries if found.
     *
     * The diff.index may either have a file (a "jcr:content" child node with a
     * "jcr:data" property), or a "diff" JSON object. For customers (in the git
     * repository), the file is much easier to construct, but when running the
     * indexing job, the nested JSON is much easier.
     *
     * @param indexDefs the set of index definitions (may be empty)
     * @param name      the name of the diff.index (either diff.index or
     *                  diff.index.optimizer)
     * @param target    the target map of diff.index definitions
     * @return the error message trying to parse the JSON file, or null
     */
    public String tryExtractDiffIndex(JsonObject indexDefs, String name, HashMap<String, JsonObject> target) {
        JsonObject diffIndex = indexDefs.getChildren().get(name);
        if (diffIndex == null) {
            return null;
        }
        // extract either the file, or the nested json
        JsonObject file = diffIndex.getChildren().get("diff.json");
        JsonObject diff;
        if (file != null) {
            // file
            JsonObject jcrContent = file.getChildren().get("jcr:content");
            if (jcrContent == null) {
                String message = "jcr:content child node is missing in diff.json";
                logAndCollectWarn(message);
                return message;
            }
            String jcrData = JsonNodeUpdater.oakStringValue(jcrContent, "jcr:data");
            try {
                diff = JsonObject.fromJson(jcrData, true);
            } catch (Exception e) {
                String message = "Illegal Json, ignoring: " + e.getMessage();
                logAndCollectWarn("Illegal Json, ignoring: {}", jcrData, e);
                return message;
            }
        } else {
            // nested json
            diff = diffIndex.getChildren().get("diff");
        }
        // store, if not empty
        if (diff != null) {
            for (Entry<String, JsonObject> e : diff.getChildren().entrySet()) {
                String key = e.getKey();
                target.put(key, mergeDiffs(target.get(key), e.getValue()));
            }
        }
        return null;
    }

    /**
     * Extract the indexes with a "mergeInfo" property and store them in the target
     * object. This is needed so that indexes that were removed from the index.diff
     * are detected (a new version is needed in this case with includedPaths
     * "/dummy").
     *
     * @param indexDefs the index definitions in the repository
     * @param target    the target map of "diff.index" definitions. for each entry
     *                  found, an empty object is added
     */
    private static void extractExistingMergedIndexes(JsonObject indexDefs, HashMap<String, JsonObject> target) {
        for (Entry<String, JsonObject> e : indexDefs.getChildren().entrySet()) {
            String key = e.getKey();
            JsonObject value = e.getValue();
            // merged indexes always contain "-custom-". Other indexes may in theory contain that term,
            // but then they do not contain "mergeInfo".
            if (key.indexOf("-custom-") < 0 || !value.getProperties().containsKey("mergeInfo")) {
                continue;
            }
            String baseName = IndexName.parse(key.substring("/oak:index/".length())).getBaseName();
            if (!target.containsKey(baseName)) {
                // if there is no entry yet for this key,
                // add a new empty object
                target.put(baseName, new JsonObject());
            }
        }
    }

    /**
     * Merge diff from "diff.index" and "diff.index.optimizer".
     * The customer can define a diff (stored in "diff.index")
     * and someone else (or the optimizer) can define one (stored in "diff.index.optimizer").
     *
     * @param a the first diff
     * @param b the second diff (overwrites entries in a)
     * @return the merged entry
     */
    public static JsonObject mergeDiffs(JsonObject a, JsonObject b) {
        if (a == null) {
            return b;
        } else if (b == null) {
            return a;
        }
        JsonObject result = JsonObject.fromJson(a.toString(), true);
        result.getProperties().putAll(b.getProperties());
        HashSet<String> both = new HashSet<>(a.getChildren().keySet());
        both.addAll(b.getChildren().keySet());
        for (String k : both) {
            result.getChildren().put(k, mergeDiffs(a.getChildren().get(k), b.getChildren().get(k)));
        }
        return result;
    }

    /**
     * Merge using the diff definition.
     *
     * If the latest customized index already matches, then
     * newImageLuceneDefinitions will remain as is. Otherwise, a new customized
     * index is added, with a "mergeInfo" property.
     *
     * Existing properties are never changed; only new properties/children are
     * added.
     *
     * @param indexName                 the name, eg. "damAssetLucene"
     * @param indexDiff                 the diff with the new properties
     * @param newImageLuceneDefinitions the new Lucene definitions (input + output)
     * @param combined                  the definitions in the repository, including
     *                                  the one in the customer repo and new ones
     *                                  (input)
     * @return whether a new version of an index was added
     */
    public boolean processMerge(String indexName, JsonObject indexDiff, JsonObject newImageLuceneDefinitions, JsonObject combined) {
        // extract the latest product index (eg. damAssetLucene-12)
        // and customized index (eg. damAssetLucene-12-custom-3) - if any
        IndexName latestProduct = null;
        String latestProductKey = null;
        IndexName latestCustomized = null;
        String latestCustomizedKey = null;
        String prefix = "/oak:index/";
        for (String key : combined.getChildren().keySet()) {
            IndexName name = IndexName.parse(key.substring(prefix.length()));
            if (!name.isVersioned()) {
                log("Ignoring unversioned index {}", name);
                continue;
            }
            if (!name.getBaseName().equals(indexName)) {
                continue;
            }
            boolean isCustom = key.indexOf("-custom-") >= 0;
            if (isCustom) {
                if (latestCustomized == null ||
                        name.compareTo(latestCustomized) > 0) {
                    latestCustomized = name;
                    latestCustomizedKey = key;
                }
            } else {
                if (latestProduct == null ||
                        name.compareTo(latestProduct) > 0) {
                    latestProduct = name;
                    latestProductKey = key;
                }
            }
        }
        log("Latest product: {}", latestProductKey);
        log("Latest customized: {}", latestCustomizedKey);
        if (latestProduct == null) {
            // if it's not a product index, then verify it's a correctly named custom index
            if (indexName.indexOf('.') >= 0) {
                // a fully custom index needs to contains a dot
                log("Fully custom index {}", indexName);
            } else {
                log("No product version for {}", indexName);
                return false;
            }
        }
        JsonObject latestProductIndex = combined.getChildren().get(latestProductKey);
        String[] includedPaths;
        if (latestProductIndex == null) {
            if (indexDiff == null || indexDiff.getProperties().isEmpty() && indexDiff.getChildren().isEmpty()) {
                // there is no customization (any more), which means a dummy index may be needed
                log("No customization for {}", indexName);
            } else {
                includedPaths = JsonNodeUpdater.oakStringArrayValue(indexDiff, "includedPaths");
                if (includesUnsupportedPaths(includedPaths)) {
                    logAndCollectWarn("New custom index {} is not supported because it contains an unsupported path ({})",
                            indexName, Arrays.toString(unsupportedIncludedPaths));
                    return false;
                }
            }
        } else {
            includedPaths = JsonNodeUpdater.oakStringArrayValue(latestProductIndex, "includedPaths");
            if (includesUnsupportedPaths(includedPaths)) {
                logAndCollectWarn("Customizing index {} is not supported because it contains an unsupported path ({})",
                        latestProductKey, Arrays.toString(unsupportedIncludedPaths));
                return false;
            }
        }

        // merge
        JsonObject merged = null;
        if (indexDiff == null) {
            // no diff definition: use to the OOTB index
            if (latestCustomized == null) {
                log("Only a product index found, nothing to do");
                return false;
            }
            merged = latestProductIndex;
        } else {
            merged = processMerge(indexName, latestProductIndex, indexDiff);
        }

        // compare to the latest version of the this index
        JsonObject latestIndexVersion = new JsonObject();
        if (latestCustomized == null) {
            latestIndexVersion = latestProductIndex;
        } else {
            latestIndexVersion = combined.getChildren().get(latestCustomizedKey);
        }
        JsonObject mergedDef = cleanedAndNormalized(switchToLuceneIfNeeded(merged));
        // compute merge checksum for later, but do not yet add
        String mergeChecksum = computeMergeChecksum(mergedDef);
        // get the merge checksum before cleaning (cleaning removes it) - if available
        String key;
        if (latestIndexVersion == null) {
            // new index
            key = prefix + indexName + "-1-custom-1";
        } else {
            String latestMergeChecksum = JsonNodeUpdater.oakStringValue(latestIndexVersion, "mergeChecksum");
            JsonObject latestDef = cleanedAndNormalized(switchToLuceneIfNeeded(latestIndexVersion));
            if (isSameIgnorePropertyOrder(mergedDef, latestDef)) {
                // normal case: no change
                // (even if checksums do not match: checksums might be missing or manipulated)
                log("Latest index matches");
                if (latestMergeChecksum != null && !latestMergeChecksum.equals(mergeChecksum)) {
                    logAndCollectWarn("Indexes do match, but checksums do not. Possibly checksum was changed: {} vs {}", latestMergeChecksum, mergeChecksum);
                    logAndCollectWarn("Index: {}, latest: {}\nmerged: {}", indexName, latestDef, mergedDef);
                }
                return false;
            }
            if (latestMergeChecksum != null && latestMergeChecksum.equals(mergeChecksum)) {
                // checksum matches, but data does not match
                // could be eg. due to numbers formatting issues (-0.0 vs 0.0, 0.001 vs 1e-3)
                // but unexpected because we do not normally have such cases
                logAndCollectWarn("Indexes do not match, but checksums match. Possible normalization issue.");
                logAndCollectWarn("Index: {}, latest: {}\nmerged: {}", indexName, latestDef, mergedDef);
                // if checksums match, we consider it a match
                return false;
            }
            LOG.info("Indexes do not match, with");
            LOG.info("Index: {}, latest: {}\nmerged: {}", indexName, latestDef, mergedDef);
            // a new merged index definition
            if (latestProduct == null) {
                // fully custom index: increment version
                key = prefix + indexName +
                        "-" + latestCustomized.getProductVersion() +
                        "-custom-" + (latestCustomized.getCustomerVersion() + 1);
            } else {
                // customized OOTB index: use the latest product as the base
                key = prefix + indexName +
                        "-" + latestProduct.getProductVersion() +
                        "-custom-";
                if (latestCustomized != null) {
                    key += (latestCustomized.getCustomerVersion() + 1);
                } else {
                    key += "1";
                }
            }
        }
        merged.getProperties().put("mergeInfo", JsopBuilder.encode(MERGE_INFO));
        merged.getProperties().put("mergeChecksum", JsopBuilder.encode(mergeChecksum));
        merged.getProperties().put("merges", "[" + JsopBuilder.encode("/oak:index/" + indexName) + "]");
        merged.getProperties().remove("reindexCount");
        merged.getProperties().remove("reindex");
        if (!deleteCopiesOutOfTheBoxIndex && indexDiff.toString().equals("{}")) {
            merged.getProperties().put("type", "\"disabled\"");
            merged.getProperties().put("mergeComment", "\"This index is superseded and can be removed\"");
        }
        newImageLuceneDefinitions.getChildren().put(key, merged);
        return true;
    }

    /**
     * Check whether the includedPaths covers unsupported paths,
     * if there are any unsupported path (eg. "/apps" or "/libs").
     * In this case, simplified index management is not supported.
     *
     * @param includedPaths the includedPaths list
     * @return true if any unsupported path is included
     */
    public boolean includesUnsupportedPaths(String[] includedPaths) {
        if (unsupportedIncludedPaths.length == 1 && "".equals(unsupportedIncludedPaths[0])) {
            // set to an empty string
            return false;
        }
        if (includedPaths == null) {
            // not set means all entries
            return true;
        }
        for (String path : includedPaths) {
            if ("/".equals(path)) {
                // all
                return true;
            }
            for (String unsupported : unsupportedIncludedPaths) {
                if (unsupported.isEmpty()) {
                    continue;
                }
                if (path.equals(unsupported) || path.startsWith(unsupported + "/")) {
                    // includedPaths matches, or starts with an unsupported path
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Compute the SHA-256 checksum of the JSON object. This is useful to detect
     * that the JSON object was not "significantly" changed, even if stored
     * somewhere and later read again. Insignificant changes include: rounding of
     * floating point numbers, re-ordering properties, things like that. Without the
     * checksum, we would risk creating a new version of a customized index each
     * time the indexing job is run, even thought the customer didn't change
     * anything.
     *
     * @param json the input
     * @return the SHA-256 checksum
     */
    private static String computeMergeChecksum(JsonObject json) {
        byte[] bytes = json.toString().getBytes(StandardCharsets.UTF_8);
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return StringUtils.convertBytesToHex(md.digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is guaranteed to be available in standard Java platforms
            throw new IllegalStateException("SHA-256 algorithm not available", e);
        }
    }

    /**
     * Switch the index from type "elasticsearch" to "lucene", if needed. This will
     * also replace all properties that have an "...@lucene" version.
     *
     * This is needed because we want to merge only the "lucene" version, to
     * simplify the merging logic. (The switch to the "elasticsearch" version
     * happens later).
     *
     * @param indexDef the index definition (is not changed by this method)
     * @return the lucene version (a new JSON object)
     */
    public static JsonObject switchToLuceneIfNeeded(JsonObject indexDef) {
        JsonObject obj = JsonObject.fromJson(indexDef.toString(), true);
        String type = JsonNodeUpdater.oakStringValue(obj, "type");
        if ("elasticsearch".equals(type) ) {
            switchToLuceneChildren(obj);
        }
        return obj;
    }

    public static void switchToLuceneChildren(JsonObject indexDef) {
        // clone the keys to avoid ConcurrentModificationException
        for (String p : new ArrayList<>(indexDef.getProperties().keySet())) {
            if (!p.endsWith("@lucene")) {
                continue;
            }
            String v = indexDef.getProperties().remove(p);
            indexDef.getProperties().put(p.substring(0, p.length() - "@lucene".length()), v);
        }
        for (String c : indexDef.getChildren().keySet()) {
            JsonObject co = indexDef.getChildren().get(c);
            switchToLuceneChildren(co);
        }
    }

    /**
     * Convert the JSON object to a new object, where index definition
     * properties that are unimportant for comparison are removed.
     * Example of important properties are "reindex", "refresh", "seed" etc.
     * The order of properties is not relevant (but the order of children is).
     *
     * @param obj the input (is not changed by the method)
     * @return a new JSON object
     */
    public static JsonObject cleanedAndNormalized(JsonObject obj) {
        obj = JsonObject.fromJson(obj.toString(), true);
        obj.getProperties().remove(":version");
        obj.getProperties().remove(":nameSeed");
        obj.getProperties().remove(":mappingVersion");
        obj.getProperties().remove("refresh");
        obj.getProperties().remove("reindexCount");
        obj.getProperties().remove("reindex");
        obj.getProperties().remove("seed");
        obj.getProperties().remove("merges");
        obj.getProperties().remove("mergeInfo");
        obj.getProperties().remove("mergeChecksum");
        for (String p : new ArrayList<>(obj.getProperties().keySet())) {
            if (p.endsWith("@lucene")) {
                obj.getProperties().remove(p);
            } else if (p.endsWith("@elasticsearch")) {
                obj.getProperties().remove(p);
            } else {
                // remove "str:", "nam:", etc if needed
                String v = obj.getProperties().get(p);
                String v2 = normalizeOakString(v);
                if (!v2.equals(v)) {
                    obj.getProperties().put(p, v2);
                }
            }
        }
        removeUUIDs(obj);
        for (Entry<String, JsonObject> e : obj.getChildren().entrySet()) {
            obj.getChildren().put(e.getKey(), cleanedAndNormalized(e.getValue()));
        }
        // re-build the properties in alphabetical order
        // (sorting the child nodes would be incorrect however, as order is significant here)
        TreeMap<String, String> props = new TreeMap<>(obj.getProperties());
        obj.getProperties().clear();
        for (Entry<String, String> e : props.entrySet()) {
            obj.getProperties().put(e.getKey(), e.getValue());
        }
        return obj;
    }

    /**
     * "Normalize" a JSON string value. Remove any "nam:" and "dat:" and "str:"
     * prefix in the value, because customers won't use them normally. (We want the
     * diff to be as simple as possible).
     *
     * @param value the value (including double quotes; eg. "str:value")
     * @return the normalized value (including double quotes)
     */
    private static String normalizeOakString(String value) {
        if (value == null || !value.startsWith("\"")) {
            // ignore numbers
            return value;
        }
        value = JsopTokenizer.decodeQuoted(value);
        if (value.startsWith("str:") || value.startsWith("nam:") || value.startsWith("dat:")) {
            value = value.substring("str:".length());
        }
        return JsopBuilder.encode(value);
    }

    /**
     * Remove all "jcr:uuid" properties (including those in children), because the
     * values might conflict. (new uuids are added later when needed).
     *
     * @param obj the JSON object where uuids will be removed.
     */
    private static void removeUUIDs(JsonObject obj) {
        obj.getProperties().remove("jcr:uuid");
        for (JsonObject c : obj.getChildren().values()) {
            removeUUIDs(c);
        }
    }

    /**
     * Merge a product index with a diff. If the product index is null, then the
     * diff needs to contain a complete custom index definition.
     *
     * @param indexName the index name (for logging)
     * @param productIndex the product index definition, or null if none
     * @param diff the diff (from the diff.index definition)
     *
     * @return the index definition of the merged index
     */
    public JsonObject processMerge(String indexName, JsonObject productIndex, JsonObject diff) {
        JsonObject result;
        boolean isNew;
        if (productIndex == null) {
            // fully custom index
            result = new JsonObject(true);
            isNew = true;
        } else {
            result = JsonObject.fromJson(productIndex.toString(), true);
            isNew = false;
        }
        mergeInto(indexName, "", diff, result, isNew);
        addPrimaryType("", result);
        return result;
    }

    /**
     * Add primary type properties where needed. For the top-level index definition,
     * this is "oak:QueryIndexDefinition", and "nt:unstructured" elsewhere.
     *
     * @param path the path (so we can call the method recursively)
     * @param json the JSON object (is changed if needed)
     */
    private static void addPrimaryType(String path, JsonObject json) {
        // all nodes need to have a node type;
        // the index definition itself (at root level) is "oak:QueryIndexDefinition",
        // and all other nodes are "nt:unstructured"
        if (!json.getProperties().containsKey("jcr:primaryType")) {
            // all nodes need to have a primary type,
            // otherwise index import will fail
            String nodeType;
            if (path.isEmpty()) {
                nodeType = "oak:QueryIndexDefinition";
            } else {
                nodeType = "nt:unstructured";
            }
            String nodeTypeValue = "nam:" + nodeType;
            json.getProperties().put("jcr:primaryType", JsopBuilder.encode(nodeTypeValue));
        }
        for (Entry<String, JsonObject> e : json.getChildren().entrySet()) {
            addPrimaryType(path + "/" + e.getKey(), e.getValue());
        }
    }

    /**
     * Merge a JSON diff into a target index definition.
     *
     * @param indexName the index name (for logging)
     * @param path the path (relative to the index)
     * @param diff the diff (what to merge)
     * @param target where to merge into
     * @param isNew whether the target node is newly created (didn't exist before)
     */
    private void mergeInto(String indexName, String path, JsonObject diff, JsonObject target,
        boolean isNewNode) {
        String pathForLogging = path.isEmpty() ? "the root" : "relative path " + path;
        for (String p : diff.getProperties().keySet()) {
            if (path.isEmpty()) {
                if ("jcr:primaryType".equals(p)) {
                    continue;
                }
            }
            if (!isNewNode) {
                // for existing nodes, we do a few more checks before the merge
                if (path.isEmpty() && REJECTED_TOP_LEVEL_PROPS_FOR_EXISTING_INDEX.contains(p)
                        && !target.getProperties().containsKey(p)) {
                    // at the top level, some properties (eg. selectionPolicy) are not allowed to be added
                    // to an existing index
                    logAndCollectWarn("{}: Ignoring new top-level property {} at {} for existing index", indexName, p, pathForLogging);
                    continue;
                }
                if (REJECTED_ADDING_TO_EXISTING_PROPERTY.contains(p) && !target.getProperties().containsKey(p)) {
                    // some properties are not allowed to be added if the node already exists
                    logAndCollectWarn("{}: Ignoring new property \"{}\" at {} for existing child", indexName, p, pathForLogging);
                    continue;
                }
            }
            if (target.getProperties().containsKey(p)) {
                // we do not currently allow to overwrite most existing properties,
                // except for:
                if (!path.isEmpty() && ALLOW_CHANGING_IN_EXISTING_PROPERTY.contains(p)) {
                    // allow overwriting the (eg.) boost value
                    target.getProperties().put(p, diff.getProperties().get(p));
                } else if (path.isEmpty() && MERGE_MULTI_VALUES_STRINGS.contains(p)) {
                    // merge includedPaths, queryPaths, and tags,
                    // such that it contains more entries
                    // (if the property is not set, we would make it more restrictive,
                    // which is not allowed)
                    TreeSet<String> oldSet = JsonNodeUpdater.getStringSet(target.getProperties().get(p));
                    TreeSet<String> newSet = JsonNodeUpdater.getStringSet(diff.getProperties().get(p));
                    TreeSet<String> mergedSet = new TreeSet<>(oldSet);
                    mergedSet.addAll(newSet);
                    JsopBuilder buff = new JsopBuilder().array();
                    for(String v : mergedSet) {
                        buff.value(v);
                    }
                    target.getProperties().put(p, buff.endArray().toString());
                } else {
                    logAndCollectWarn("{}: Ignoring existing property \"{}\" at {}", indexName, p, pathForLogging);
                }
            } else {
                target.getProperties().put(p, diff.getProperties().get(p));
            }
        }
        for (String c : diff.getChildren().keySet()) {
            boolean childIsNew;
            String targetChildName = c;
            if (!target.getChildren().containsKey(c)) {
                childIsNew = true;
                if (path.endsWith("/properties")) {
                    // search for a property with the same "name" value
                    String propertyName = diff.getChildren().get(c).getProperties().get("name");
                    if (propertyName != null) {
                        propertyName = JsonNodeUpdater.oakStringValue(propertyName);
                        String c2 = getChildWithKeyValuePair(target, "name", propertyName);
                        if (c2 != null) {
                            targetChildName = c2;
                        }
                    }
                    // search for a property with the same "function" value
                    String function = diff.getChildren().get(c).getProperties().get("function");
                    if (function != null) {
                        function = JsonNodeUpdater.oakStringValue(function);
                        String c2 = getChildWithKeyValuePair(target, "function", function);
                        if (c2 != null) {
                            targetChildName = c2;
                        }
                    }
                }
                if (targetChildName.equals(c)) {
                    // only create the child (properties are added below)
                    target.getChildren().put(c, new JsonObject());
                }
            } else {
                childIsNew = false;
            }
            mergeInto(indexName, path + "/" + targetChildName, diff.getChildren().get(c), target.getChildren().get(targetChildName), childIsNew);
        }
        if (target.getProperties().isEmpty() && target.getChildren().isEmpty()) {
            if (deleteCreatesDummyIndex) {
                // dummy index
                target.getProperties().put("async", "\"async\"");
                target.getProperties().put("includedPaths", "\"/dummy\"");
                target.getProperties().put("queryPaths", "\"/dummy\"");
                target.getProperties().put("type", "\"lucene\"");
                JsopBuilder buff = new JsopBuilder();
                buff.object().
                    key("properties").object().
                        key("dummy").object().
                            key("name").value("dummy").
                            key("propertyIndex").value(true).
                        endObject().
                    endObject().
                endObject();
                JsonObject indexRules = JsonObject.fromJson(buff.toString(), true);
                target.getChildren().put("indexRules", indexRules);
            } else {
                target.getProperties().put("type", "\"disabled\"");
            }
        }
    }

    /**
     * Find a child node that contains a property with the given key and value.
     * This is used during merging to find an existing index rule property
     * definition that matches a given "name" or "function" value, so that the
     * diff can be applied to the correct child even if the child node name differs.
     *
     * @param obj the parent JSON object whose children are searched
     * @param key the property key to match (e.g. "name" or "function")
     * @param value the expected property value (already converted via oakStringValue)
     * @return the name of the first matching child, or null if no match is found
     */
    public static String getChildWithKeyValuePair(JsonObject obj, String key, String value) {
        for (Entry<String, JsonObject> c : obj.getChildren().entrySet()) {
            String v2 = c.getValue().getProperties().get(key);
            if (v2 == null) {
                continue;
            }
            v2 = JsonNodeUpdater.oakStringValue(v2);
            if (value.equals(v2)) {
                return c.getKey();
            }
        }
        return null;
    }

    /**
     * Compare two JSON object, ignoring the order of properties. (The order of
     * children is however significant).
     *
     * This is done in addition to the checksum comparison, because the in theory
     * the customer might change the checksum (it is not read-only as read-only
     * values are not supported). We do not rely on the comparison, but if comparison
     * and checksum comparison do not match, we log a warning.
     *
     * @param a the first object
     * @param b the second object
     * @return true if the keys and values are equal
     */
    public boolean isSameIgnorePropertyOrder(JsonObject a, JsonObject b) {
        if (!a.getChildren().keySet().equals(b.getChildren().keySet())) {
            log("Child (order) difference: {} vs {}",
                    a.getChildren().keySet(), b.getChildren().keySet());
            return false;
        }
        for (String k : a.getChildren().keySet()) {
            if (!isSameIgnorePropertyOrder(
                    a.getChildren().get(k), b.getChildren().get(k))) {
                return false;
            }
        }
        TreeMap<String, String> pa = new TreeMap<>(a.getProperties());
        TreeMap<String, String> pb = new TreeMap<>(b.getProperties());
        if (!pa.toString().equals(pb.toString())) {
            log("Property value difference: {} vs {}", pa.toString(), pb.toString());
        }
        return pa.toString().equals(pb.toString());
    }

    /**
     * Read a diff.index from the repository, if it exists.
     * This is needed because the build-transform job doesn't have this
     * data: it is only available in the writeable repository.
     *
     * @param repositoryNodeStore the node store
     * @param name the name (diff.index, diff.index.optimize,...)
     * @return a map, possibly with a single entry with this key
     */
    public Map<String, JsonObject> readDiffIndex(NodeStore repositoryNodeStore, String name) {
        HashMap<String, JsonObject> map = new HashMap<>();
        NodeState root = repositoryNodeStore.getRoot();
        String indexPath = "/oak:index/" + name;
        NodeState idxState = NodeStateUtils.getNode(root, indexPath);
        log("Searching index {}: found={}", indexPath, idxState.exists());
        if (!idxState.exists()) {
            return map;
        }
        JsopBuilder builder = new JsopBuilder();
        String filter = "{\"properties\":[\"*\", \"-:childOrder\"],\"nodes\":[\"*\", \"-:*\"]}";
        JsonSerializer serializer = new JsonSerializer(builder, filter, new Base64BlobSerializer());
        serializer.serialize(idxState);
        JsonObject jsonObj = JsonObject.fromJson(builder.toString(), true);
        jsonObj = cleanedAndNormalized(jsonObj);
        log("Found {}", jsonObj.toString());
        map.put(indexPath, jsonObj);
        return map;
    }

    private void log(String format, Object... arguments) {
        if (logAtInfoLevel) {
            LOG.info(format, arguments);
        } else {
            LOG.debug(format, arguments);
        }
    }

    /**
     * Log a warning message and store it in a size-limited queue.
     * The queue keeps the oldest entries and is limited to 100 entries or 1 MB total size.
     *
     * @param format the log message format
     * @param arguments the log message arguments
     */
    public void logAndCollectWarn(String format, Object... arguments) {
        String message = org.slf4j.helpers.MessageFormatter.arrayFormat(format, arguments).getMessage();
        LOG.warn(message);
        synchronized (warnings) {
            int messageSize = message.getBytes(StandardCharsets.UTF_8).length;
            if (warnings.size() < MAX_WARNINGS && warningsSize + messageSize <= MAX_WARNINGS_SIZE) {
                warnings.add(message);
                warningsSize += messageSize;
            }
        }
    }

    /**
     * Get and clear all collected warnings.
     *
     * @return a list of warning messages (oldest first)
     */
    public List<String> getAndClearWarnings() {
        synchronized (warnings) {
            List<String> result = new ArrayList<>(warnings);
            warnings.clear();
            warningsSize = 0;
            return result;
        }
    }

    public DiffIndexMerger setUnsupportedIncludedPaths(String[] unsupportedIncludedPaths) {
        this.unsupportedIncludedPaths = unsupportedIncludedPaths;
        return this;
    }

    public DiffIndexMerger setDeleteCreatesDummyIndex(boolean deleteCreatesDummyIndex) {
        this.deleteCreatesDummyIndex = deleteCreatesDummyIndex;
        return this;
    }

    public DiffIndexMerger setDeleteCopiesOutOfTheBoxIndex(boolean deleteCopiesOutOfTheBoxIndex) {
        this.deleteCopiesOutOfTheBoxIndex = deleteCopiesOutOfTheBoxIndex;
        return this;
    }

    public DiffIndexMerger setLogAtInfoLevel(boolean logAtInfoLevel) {
        this.logAtInfoLevel = logAtInfoLevel;
        return this;
    }

}
