package org.apache.jackrabbit.oak.composite;

import static org.apache.jackrabbit.oak.api.QueryEngine.NO_MAPPINGS;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.json.Base64BlobSerializer;
import org.apache.jackrabbit.oak.json.JsonSerializer;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class OakQueryIndexAnalyzer {

    public final static String PROPERTY_CONDITION = "<propertyCondition>";
    public final static String LANGUAGE_SQL2 = "JCR-SQL2";
    public final static String LANGUAGE_XPATH = "xpath";

    private NodeStore nodeStore;
    private Root root;
    private QueryEngine qe;

    OakQueryIndexAnalyzer(NodeStore nodeStore, ContentSession session) {
        this.nodeStore = nodeStore;
        this.root = session.getLatestRoot();
        this.qe = root.getQueryEngine();
    }

    private static String getQueryLanguage(String statement) {
        statement = statement.trim().toLowerCase(Locale.ROOT);
        if (statement.startsWith("select")) {
            return LANGUAGE_SQL2;
        } else if (statement.startsWith("(") || statement.startsWith("/")) {
            return LANGUAGE_XPATH;
        }
        throw new IllegalArgumentException("Can not detect query language from query; query needs to start with 'select' or '/': " + statement);
    }

    public boolean isQueryIndexed(String statement) throws ParseException {
        QueryStatistics stats =  getQueryStats(statement);
        return stats.estimatedEntries < 1000;
    }

    private QueryStatistics getQueryStats(String statement) throws ParseException {
        Map<String, PropertyValue> sv = Map.of();
        String planQuery = "explain " + statement;
        String language = getQueryLanguage(statement);
        Result queryResult = qe.executeQuery(planQuery, language, sv, NO_MAPPINGS);
        QueryStatistics stats = new QueryStatistics();
        for (ResultRow r : queryResult.getRows()) {
            String plan = r.getValues()[0].getValue(Type.STRING);
            String[] lines = plan.split("\n");
            for(String line : lines) {
                line = line.trim();
                if (line.startsWith("indexDefinition:")) {
                    stats.indexDefinition = line.split(":")[1].trim();
                } else if (line.startsWith("estimatedEntries:")) {
                    try {
                        stats.estimatedEntries = Double.parseDouble(line.split(":")[1].trim());
                    } catch (NumberFormatException e) {
                        stats.error = e.toString();
                    }
                } else if (line.startsWith("luceneQuery:")) {
                    stats.backendQuery = line.split(":")[1].trim();
                }
            }
            // System.out.println(plan);
        }
        return stats;
    }

    private static class QueryStatistics {
        String indexDefinition = "<traverse>";
        double estimatedEntries = Double.POSITIVE_INFINITY;
        String backendQuery = null;
        String error = null;
    }

    private static String cleanPropertyName(String name) {
        if (name == null) {
            return null;
        }
        name = JsopTokenizer.decodeQuoted(name);
        name = name.trim();
        if (name.startsWith("[") && name.endsWith("]")) {
            name = name.substring(1, name.length() - 2).trim();
        }
        if (name.startsWith("^") || name.endsWith("$") || name.indexOf("[") >= 0) {
            // forgot to set isRegexp
            return null;
        }
        if (name.indexOf("*") >= 0) {
            // eg. jcr:content/*/child
            return null;
        }
        while (name.startsWith("./")) {
            name = name.substring("./".length()).trim();
        }
        if (name.indexOf("./") >= 0) {
            // this includes "../", "child/./child", and "child/../child"
            return null;
        }
        if (name.startsWith("str:") || name.startsWith("pat:") || name.startsWith("nam:")) {
            name = name.substring("str:".length()).trim();
        }
        if (name.startsWith("/")) {
            name = name.substring(1).trim();
        }
        if (name.startsWith("@")) {
            name = name.substring(1).trim();
        }
        if (name.indexOf("(") >= 0) {
            // functions
            return null;
        }
        if (name.startsWith(":")) {
            // ":nodeName", ":name"
            return null;
        }
        if (name.isEmpty()) {
            return null;
        }
        return name.trim();
    }

    public List<String> getIndexedProperties(String statement) throws ParseException {
        return getIndexedProperties(statement, false);
    }

    public List<String> getOrderedProperties(String statement) throws ParseException {
        return getIndexedProperties(statement, true);
    }

    private List<String> getIndexedProperties(String statement, boolean ordered) throws ParseException {
        String language = getQueryLanguage(statement);
        int startIndex = statement.indexOf(PROPERTY_CONDITION);
        if (startIndex < 0) {
            throw new IllegalArgumentException("The query needs to contain " + PROPERTY_CONDITION);
        }
        NodeState idxState = NodeStateUtils.getNode(nodeStore.getRoot(), "oak:index");
        JsopBuilder json = new JsopBuilder();
        json.object();
        json.key("oak:index");
        String filter = "{\"properties\":[\"*\", \"-:childOrder\"],\"nodes\":[\"*\", \"-:*\"]}";
        JsonSerializer serializer = new JsonSerializer(json, filter, new Base64BlobSerializer());
        serializer.serialize(idxState);
        json.endObject();
        String indexDefs = json.toString();
        // System.out.println(indexDefs);
        JsonObject jsonObj = JsonObject.fromJson(indexDefs, true);
        JsonObject oakIndex = jsonObj.getChildren().get("oak:index");
        TreeSet<String> candidateProperties = new TreeSet<>();
        for (String index : oakIndex.getChildren().keySet()) {
            JsonObject idx = oakIndex.getChildren().get(index);
            JsonObject rules = idx.getChildren().get("indexRules");
            if (rules == null) {
                continue;
            }
            for (String typeName : rules.getChildren().keySet()) {
                JsonObject type = rules.getChildren().get(typeName);
                JsonObject properties = type.getChildren().get("properties");
                if (properties == null) {
                    continue;
                }
                for (String prop : properties.getChildren().keySet()) {
                    JsonObject p = properties.getChildren().get(prop);
                    if (ordered) {
                        String orderedValue = p.getProperties().get("ordered");
                        if (orderedValue == null) {
                            continue;
                        }
                    }
                    String regex = p.getProperties().get("isRegexp");
                    if (regex != null && "true".equals(regex)) {
                        continue;
                    }
                    String n = p.getProperties().get("name");
                    String propName = cleanPropertyName(n);
                    if (propName != null) {
                        candidateProperties.add(propName);
                    }
                }
            }
        }
        ArrayList<String> result = new ArrayList<>();
        for(String candidate : candidateProperties) {
            String condition;
            if (LANGUAGE_SQL2.equals(language)) {
                condition = "[" + candidate + "] = 'test'";
            } else {
                String xpathProperty;
                int lastSlash = candidate.lastIndexOf('/');
                if (lastSlash < 0) {
                    xpathProperty = "@" + candidate;
                } else {
                    xpathProperty = candidate.substring(0, lastSlash + 1) + "@" + candidate.substring(lastSlash + 1);
                }
                condition = xpathProperty + " = 'test'";
            }
            String query = statement.replaceAll(Pattern.quote(PROPERTY_CONDITION), condition);
            if (isQueryIndexed(query)) {
                result.add(candidate);
            }
        }
        return result;
    }

}
