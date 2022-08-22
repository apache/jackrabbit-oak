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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.IndexName;

/**
 * The index diff tools allows to compare and merge indexes
 */
public class IndexDiff {

    private static final String OAK_INDEX = "/oak:index/";

    static JsonObject extract(String extractFile, String indexName) {
        JsonObject indexDefs = parseIndexDefinitions(extractFile);
        JsonObject index = indexDefs.getChildren().get(indexName);
        removeUninterestingIndexProperties(indexDefs);
        simplify(index);
        return index;
    }

    static void extractAll(String extractFile, String extractTargetDirectory) {
        new File(extractTargetDirectory).mkdirs();
        JsonObject indexDefs = parseIndexDefinitions(extractFile);
        removeUninterestingIndexProperties(indexDefs);
        sortPropertiesByName(indexDefs);
        for (String child : indexDefs.getChildren().keySet()) {
            JsonObject index = indexDefs.getChildren().get(child);
            simplify(index);
            String fileName = child.replaceAll(OAK_INDEX, "");
            fileName = fileName.replace(':', '-');
            Path p = Paths.get(extractTargetDirectory, fileName + ".json");
            writeFile(p, index);
        }
    }

    private static void writeFile(Path p, JsonObject json) {
        try {
            Files.write(p, json.toString().getBytes());
        } catch (IOException e) {
            throw new IllegalStateException("Error writing file: " + p, e);
        }
    }

    static JsonObject collectCustomizations(String directory) {
        Path indexPath = Paths.get(directory);
        JsonObject target = new JsonObject(true);
        collectCustomizationsInDirectory(indexPath, target);
        return target;
    }

    static JsonObject mergeIndexes(String directory, String newIndexFile) {
        JsonObject newIndex = null;
        if (newIndexFile != null && !newIndexFile.isEmpty()) {
            newIndex = parseIndexDefinitions(newIndexFile);
        }
        Path indexPath = Paths.get(directory);
        JsonObject target = new JsonObject(true);
        mergeIndexesInDirectory(indexPath, newIndex, target);
        for(String key : target.getChildren().keySet()) {
            JsonObject c = target.getChildren().get(key);
            removeUninterestingIndexProperties(c);
            sortPropertiesByName(c);
            simplify(c);
            target.getChildren().put(key, c);
        }
        return target;
    }

    static void mergeIndex(String oldIndexFile, String newIndexFile, String targetDirectory) {
        JsonObject oldIndexes = parseIndexDefinitions(oldIndexFile);
        removeUninterestingIndexProperties(oldIndexes);
        sortPropertiesByName(oldIndexes);
        simplify(oldIndexes);

        JsonObject newIndexes = parseIndexDefinitions(newIndexFile);
        removeUninterestingIndexProperties(newIndexes);
        sortPropertiesByName(newIndexes);
        simplify(newIndexes);

        List<IndexName> newNames = newIndexes.getChildren().keySet().stream().map(s -> IndexName.parse(s))
                .collect(Collectors.toList());
        List<IndexName> allNames = oldIndexes.getChildren().keySet().stream().map(s -> IndexName.parse(s))
                .collect(Collectors.toList());

        for (IndexName n : newNames) {
            if (n.getCustomerVersion() == 0) {
                IndexName latest = n.getLatestCustomized(allNames);
                IndexName ancestor = n.getLatestProduct(allNames);
                if (latest != null && ancestor != null) {
                    if (n.compareTo(latest) <= 0 || n.compareTo(ancestor) <= 0) {
                        // ignore older versions of indexes
                        continue;
                    }
                    JsonObject latestCustomized = oldIndexes.getChildren().get(latest.getNodeName());
                    String fileName = PathUtils.getName(latest.getNodeName());
                    writeFile(Paths.get(targetDirectory, fileName + ".json"),
                            addParent(latest.getNodeName(), latestCustomized));

                    JsonObject latestAncestor = oldIndexes.getChildren().get(ancestor.getNodeName());
                    fileName = PathUtils.getName(ancestor.getNodeName());
                    writeFile(Paths.get(targetDirectory, fileName + ".json"),
                            addParent(ancestor.getNodeName(), latestAncestor));

                    JsonObject newProduct = newIndexes.getChildren().get(n.getNodeName());
                    fileName = PathUtils.getName(n.getNodeName());
                    writeFile(Paths.get(targetDirectory, fileName + ".json"),
                            addParent(n.getNodeName(), newProduct));

                    JsonObject oldCustomizations = new JsonObject(true);
                    compareIndexes("", latestAncestor, latestCustomized, oldCustomizations);
                    // the old product index might be disabled
                    oldCustomizations.getChildren().remove("type");
                    writeFile(Paths.get(targetDirectory, "oldCustomizations.json"),
                            oldCustomizations);

                    JsonObject productChanges = new JsonObject(true);
                    compareIndexes("", latestAncestor, newProduct, productChanges);
                    writeFile(Paths.get(targetDirectory, "productChanges.json"),
                            productChanges);

                    try {
                        JsonObject merged = IndexDefMergerUtils.merge(
                                "", latestAncestor,
                                latest.getNodeName(), latestCustomized,
                                newProduct, n.getNodeName());
                        fileName = PathUtils.getName(n.nextCustomizedName());
                        writeFile(Paths.get(targetDirectory, fileName + ".json"),
                                addParent(n.nextCustomizedName(), merged));

                        JsonObject newCustomizations = new JsonObject(true);
                        compareIndexes("", newProduct, merged, newCustomizations);
                        writeFile(Paths.get(targetDirectory, "newCustomizations.json"),
                                newCustomizations);

                        JsonObject changes = new JsonObject(true);
                        compareIndexes("", oldCustomizations, newCustomizations, changes);
                        writeFile(Paths.get(targetDirectory, "changes.json"),
                                changes);

                    } catch (UnsupportedOperationException e) {
                        throw new UnsupportedOperationException("Index: " + n.getNodeName() + ": " + e.getMessage(), e);
                    }
                }
            }
        }
    }

    private static JsonObject addParent(String key, JsonObject obj) {
        JsonObject result = new JsonObject(true);
        result.getChildren().put(key, obj);
        return result;
    }

    static JsonObject compareIndexes(String directory, String index1, String index2) {
        Path indexPath = Paths.get(directory);
        JsonObject target = new JsonObject(true);
        compareIndexesInDirectory(indexPath, index1, index2, target);
        return target;
    }

    public static JsonObject compareIndexesAgainstBase(String directory, String indexBaseFile) {
        Path indexPath = Paths.get(directory);
        JsonObject baseIndexes = parseIndexDefinitions(indexBaseFile);
        removeUninterestingIndexProperties(baseIndexes);
        simplify(baseIndexes, true);
        JsonObject target = new JsonObject(true);
        compareIndexesInDirectory(indexPath, baseIndexes, target);
        return target;
    }

    private static Stream<Path> indexFiles(Path indexPath) {
        try {
            return Files.walk(indexPath).
            filter(path -> Files.isRegularFile(path)).
            filter(path -> path.toString().endsWith(".json")).
            filter(path -> !path.toString().endsWith("allnamespaces.json")).
            filter(path -> !path.toString().endsWith("-info.json")).
            filter(path -> !path.toString().endsWith("-stats.json"));
        } catch (IOException e) {
            throw new IllegalArgumentException("Error reading from " + indexPath, e);
        }
    }

    private static void sortPropertiesByName(JsonObject obj) {
        ArrayList<String> props = new ArrayList<>(obj.getProperties().keySet());
        if (!props.isEmpty()) {
            props.sort(null);
            for(String key : props) {
                String value = obj.getProperties().remove(key);
                obj.getProperties().put(key, value);
            }
        }
        for(String child : obj.getChildren().keySet()) {
            JsonObject c = obj.getChildren().get(child);
            sortPropertiesByName(c);
        }
    }

    public static JsonObject compareIndexesInFile(Path indexPath, String index1, String index2) {
        JsonObject indexDefinitions = IndexDiff.parseIndexDefinitions(indexPath.toString());
        JsonObject target = new JsonObject(true);
        compareIndexes(indexDefinitions, "", indexPath.toString(), index1, index2, target);
        return target;
    }

    private static void compareIndexesInDirectory(Path indexPath, String index1, String index2,
            JsonObject target) {
        if (Files.isDirectory(indexPath)) {
            indexFiles(indexPath).forEach(path -> {
                JsonObject indexDefinitions = IndexDiff.parseIndexDefinitions(path.toString());
                compareIndexes(indexDefinitions, indexPath.toString(), path.toString(), index1, index2, target);
            });
        } else {
            JsonObject allIndexDefinitions = IndexDiff.parseIndexDefinitions(indexPath.toString());
            for(String key : allIndexDefinitions.getChildren().keySet()) {
                JsonObject indexDefinitions = allIndexDefinitions.getChildren().get(key);
                compareIndexes(indexDefinitions, "", key, index1, index2, target);
            }
        }
    }

    private static void compareIndexesInDirectory(Path indexPath, JsonObject baseIndexes, JsonObject target) {
        if (Files.isDirectory(indexPath)) {
            indexFiles(indexPath).forEach(path -> {
                JsonObject indexDefinitions = IndexDiff.parseIndexDefinitions(path.toString());
                removeUninterestingIndexProperties(indexDefinitions);
                simplify(indexDefinitions, true);
                compareIndexes(indexDefinitions, indexPath.toString(), path.toString(), baseIndexes, target);
            });
        } else {
            throw new IllegalArgumentException("Not a directory: " + indexPath);
        }
    }

    private static void collectCustomizationsInDirectory(Path indexPath, JsonObject target) {
        indexFiles(indexPath).forEach(path -> {
            JsonObject indexDefinitions = IndexDiff.parseIndexDefinitions(path.toString());
            showCustomIndexes(indexDefinitions, indexPath.toString(), path.toString(), target);
        });
    }

    private static void mergeIndexesInDirectory(Path indexPath, JsonObject newIndex, JsonObject target) {
        indexFiles(indexPath).forEach(path -> {
            JsonObject indexDefinitions = IndexDiff.parseIndexDefinitions(path.toString());
            simplify(indexDefinitions);
            mergeIndexes(indexDefinitions, indexPath.toString(), path.toString(), newIndex, target);
        });
    }

    private static void mergeIndexes(JsonObject indexeDefinitions, String basePath, String fileName, JsonObject newIndexes, JsonObject target) {
        JsonObject targetFile = new JsonObject(true);
        if (newIndexes != null) {
            for (String newIndexKey : newIndexes.getChildren().keySet()) {
                if (indexeDefinitions.getChildren().containsKey(newIndexKey)) {
                    targetFile.getProperties().put(newIndexKey, JsopBuilder.encode("WARNING: already exists"));
                }
            }
        } else {
            newIndexes = new JsonObject(true);
        }
        // the superseded indexes of the old repository
        List<String> supersededKeys = new ArrayList<>(IndexMerge.getSupersededIndexDefs(indexeDefinitions));
        Collections.sort(supersededKeys);

        // keep only new indexes that are not superseded
        Map<String, JsonObject> indexMap = indexeDefinitions.getChildren();
        for (String superseded : supersededKeys) {
            indexMap.remove(superseded);
        }
        Set<String> indexKeys = indexeDefinitions.getChildren().keySet();
        try {
            IndexDefMergerUtils.merge(newIndexes, indexeDefinitions);
            Set<String> newIndexKeys =  new HashSet<>(newIndexes.getChildren().keySet());
            newIndexKeys.removeAll(indexKeys);
            for (String newIndexKey : newIndexKeys) {
                JsonObject merged = newIndexes.getChildren().get(newIndexKey);
                if (merged != null) {
                    targetFile.getChildren().put(newIndexKey, merged);
                }
            }
        } catch (UnsupportedOperationException e) {
            e.printStackTrace();
            targetFile.getProperties().put("failed", JsopBuilder.encode(e.toString()));
        }
        addIfNotEmpty(basePath, fileName, targetFile, target);
    }

    private static void addIfNotEmpty(String basePath, String fileName, JsonObject targetFile, JsonObject target) {
        if (!targetFile.getProperties().isEmpty() || !targetFile.getChildren().isEmpty()) {
            String f = fileName;
            if (f.startsWith(basePath)) {
                f = f.substring(basePath.length());
            }
            target.getChildren().put(f, targetFile);
        }
    }

    private static void showCustomIndexes(JsonObject indexDefinitions, String basePath, String fileName, JsonObject target) {
        JsonObject targetFile = new JsonObject(true);
        processAndRemoveIllegalIndexNames(indexDefinitions, targetFile);
        removeUninterestingIndex(indexDefinitions);
        removeUninterestingIndexProperties(indexDefinitions);
        removeUnusedIndexes(indexDefinitions);
        for(String k : indexDefinitions.getChildren().keySet()) {
            if (!k.startsWith(OAK_INDEX)) {
                targetFile.getProperties().put(k, JsopBuilder.encode("WARNING: Index not under " + OAK_INDEX));
                continue;
            }
            if (!k.contains("-custom-")) {
                continue;
            }
            listNewAndCustomizedIndexes(indexDefinitions, k, targetFile);
        }
        addIfNotEmpty(basePath, fileName, targetFile, target);
    }

    private static void compareIndexes(JsonObject indexDefinitions, String basePath, String fileName, String index1, String index2, JsonObject target) {
        JsonObject targetFile = new JsonObject(true);
        JsonObject i1 = indexDefinitions.getChildren().get(index1);
        JsonObject i2 = indexDefinitions.getChildren().get(index2);
        if (i1 != null && i2 != null) {
            compareIndexes("", i1, i2, targetFile);
        }
        addIfNotEmpty(basePath, fileName, targetFile, target);
    }

    private static void compareIndexes(JsonObject indexDefinitions, String basePath, String fileName,
            JsonObject baseIndexes, JsonObject target) {
        JsonObject targetFile = new JsonObject(true);
        for (String indexName : baseIndexes.getChildren().keySet()) {
            JsonObject baseIndex = baseIndexes.getChildren().get(indexName);
            JsonObject compareIndex = indexDefinitions.getChildren().get(indexName);
            if (compareIndex != null) {
                JsonObject targetIndex = new JsonObject(true);
                compareIndexes("", baseIndex, compareIndex, targetIndex);
                if (!targetIndex.getChildren().isEmpty()) {
                    targetFile.getChildren().put(indexName, targetIndex);
                }
            }
        }
        addIfNotEmpty(basePath, fileName, targetFile, target);
    }

    private static void listNewAndCustomizedIndexes(JsonObject indexDefinitions, String indexNodeName, JsonObject target) {
        JsonObject index = indexDefinitions.getChildren().get(indexNodeName);
        String nodeName = indexNodeName.substring(OAK_INDEX.length());
        IndexName indexName = IndexName.parse(nodeName);
        String ootb = indexName.getBaseName();
        if (indexName.getProductVersion() > 1) {
            ootb += "-" + indexName.getProductVersion();
        }
        simplify(indexDefinitions);
        JsonObject ootbIndex = indexDefinitions.getChildren().get(OAK_INDEX + ootb);
        if (ootbIndex != null) {
            JsonObject targetCustom = new JsonObject(true);
            targetCustom.getProperties().put("customizes", JsopBuilder.encode(OAK_INDEX + ootb));
            target.getChildren().put(indexNodeName, targetCustom);
            compareIndexes("", ootbIndex, index, targetCustom);
        } else {
            target.getProperties().put(indexNodeName, JsopBuilder.encode("new"));
        }
    }

    private static void processAndRemoveIllegalIndexNames(JsonObject indexDefinitions, JsonObject target) {
        Set<String> indexes = new HashSet<>(indexDefinitions.getChildren().keySet());
        for(String k : indexes) {
            if (!k.startsWith("/oak:index/")) {
                continue;
            }
            String nodeName = k.substring("/oak:index/".length());
            IndexName indexName = IndexName.parse(nodeName);
            if (!indexName.isLegal()) {
                target.getProperties().put(k, JsopBuilder.encode("WARNING: Invalid name"));
                indexDefinitions.getChildren().remove(k);
            }
        }
    }

    private static void removeUninterestingIndex(JsonObject indexDefinitions) {
    }

    private static void compareIndexes(String path, JsonObject ootb, JsonObject custom, JsonObject target) {
        LinkedHashMap<String, Boolean> properties = new LinkedHashMap<>();
        addAllProperties(ootb, properties);
        addAllProperties(custom, properties);
        for (String k : properties.keySet()) {
            String op = ootb.getProperties().get(k);
            String cp = custom.getProperties().get(k);
            if (!Objects.equals(op, cp)) {
                JsonObject change = new JsonObject(true);
                if (op != null) {
                    change.getProperties().put("old", op);
                }
                if (cp != null) {
                    change.getProperties().put("new", cp);
                }
                target.getChildren().put(path + k, change);
            }
        }
        LinkedHashMap<String, Boolean> children = new LinkedHashMap<>();
        addAllChildren(ootb, children);
        addAllChildren(custom, children);
        for (String k : children.keySet()) {
            JsonObject oc = ootb.getChildren().get(k);
            JsonObject cc = custom.getChildren().get(k);
            if (!isSameJson(oc, cc)) {
                if (oc == null) {
                    target.getProperties().put(path + k, JsopBuilder.encode("added"));
                } else if (cc == null) {
                    target.getProperties().put(path + k, JsopBuilder.encode("removed"));
                } else {
                    compareIndexes(path + k + "/", oc, cc, target);
                }
            }
        }
        compareOrder(path, ootb, custom, target);
    }

    private static void addAllChildren(JsonObject source, LinkedHashMap<String, Boolean> target) {
        for(String k : source.getChildren().keySet()) {
            target.put(k,  true);
        }
    }

    private static void compareOrder(String path, JsonObject ootb, JsonObject custom, JsonObject target) {
        // list of entries, sorted by how they appear in the ootb case
        ArrayList<String> bothSortedByOotb = new ArrayList<>();
        for(String k : ootb.getChildren().keySet()) {
            if (custom.getChildren().containsKey(k)) {
                bothSortedByOotb.add(k);
            }
        }
        // list of entries, sorted by how they appear in the custom case
        ArrayList<String> bothSortedByCustom = new ArrayList<>();
        for(String k : custom.getChildren().keySet()) {
            if (ootb.getChildren().containsKey(k)) {
                bothSortedByCustom.add(k);
            }
        }
        if (!bothSortedByOotb.toString().equals(bothSortedByCustom.toString())) {
            JsonObject change = new JsonObject(true);
            change.getProperties().put("warning", JsopBuilder.encode("WARNING: Order is different"));
            change.getProperties().put("ootb", JsopBuilder.encode(bothSortedByOotb.toString()));
            change.getProperties().put("custom", JsopBuilder.encode(bothSortedByCustom.toString()));
            target.getChildren().put(path + "<order>", change);
        }
    }

    private static boolean isSameJson(JsonObject a, JsonObject b) {
        if (a == null || b == null) {
            return a == null && b == null;
        }
        return a.toString().equals(b.toString());
    }

    private static void addAllProperties(JsonObject source, LinkedHashMap<String, Boolean> target) {
        for(String k : source.getProperties().keySet()) {
            target.put(k,  true);
        }
    }

    private static void removeUnusedIndexes(JsonObject indexDefinitions) {
        HashMap<String, IndexName> latest = new HashMap<>();
        Set<String> indexes = new HashSet<>(indexDefinitions.getChildren().keySet());
        for(String k : indexes) {
            if (!k.startsWith("/oak:index/")) {
                continue;
            }
            String nodeName = k.substring("/oak:index/".length());
            IndexName indexName = IndexName.parse(nodeName);
            String baseName = indexName.getBaseName();
            IndexName old = latest.get(baseName);
            if (old == null) {
                latest.put(baseName, indexName);
            } else {
                if (old.compareTo(indexName) < 0) {
                    if (old.getCustomerVersion() > 0) {
                        indexDefinitions.getChildren().remove("/oak:index/" + old.getNodeName());
                    }
                    latest.put(baseName, indexName);
                } else {
                    indexDefinitions.getChildren().remove("/oak:index/" + nodeName);
                }
            }
        }
    }

    private static void removeUninterestingIndexProperties(JsonObject indexDefinitions) {
        for(String k : indexDefinitions.getChildren().keySet()) {
            JsonObject indexDef = indexDefinitions.getChildren().get(k);
            indexDef.getProperties().remove("reindexCount");
            indexDef.getProperties().remove("reindex");
            indexDef.getProperties().remove("seed");
            indexDef.getProperties().remove(":version");
        }
    }

    private static void simplify(JsonObject json) {
        simplify(json, false);
    }

    private static void simplify(JsonObject json, boolean thoroughly) {
        for(String k : json.getChildren().keySet()) {
            JsonObject child = json.getChildren().get(k);
            simplify(child, thoroughly);

            // the following properties are not strictly needed for display,
            // but we keep them by default, to avoid issues with validation
            if (thoroughly) {
                child.getProperties().remove("jcr:created");
                child.getProperties().remove("jcr:createdBy");
                child.getProperties().remove("jcr:lastModified");
                child.getProperties().remove("jcr:lastModifiedBy");
            }

            // the UUID we remove, because duplicate UUIDs are not allowed
            child.getProperties().remove("jcr:uuid");
            for(String p : child.getProperties().keySet()) {
                String v = child.getProperties().get(p);
                if (v.startsWith("\"str:") || v.startsWith("\"nam:")) {
                    v = "\"" + v.substring(5);
                    child.getProperties().put(p, v);
                } else if (v.startsWith("[") && v.contains("\"nam:")) {
                    v = v.replaceAll("\\[\"nam:", "\\[\"");
                    v = v.replaceAll(", \"nam:", ", \"");
                    child.getProperties().put(p, v);
                } else if (v.startsWith("\":blobId:")) {
                    String base64 = v.substring(9, v.length() - 1);
                    String clear = new String(java.util.Base64.getDecoder().decode(base64), StandardCharsets.UTF_8);
                    v = JsopBuilder.encode(clear);
                    // we don't update the property, otherwise importing the index
                    // would change the type
                    // child.getProperties().put(p, v);
                }
            }
        }
    }

    private static JsonObject parseIndexDefinitions(String jsonFileName) {
        try {
            String json = new String(Files.readAllBytes(Paths.get(jsonFileName)));
            return JsonObject.fromJson(json, true);
        } catch (Exception e) {
            throw new IllegalStateException("Error parsing file: " + jsonFileName, e);
        }
    }

}
