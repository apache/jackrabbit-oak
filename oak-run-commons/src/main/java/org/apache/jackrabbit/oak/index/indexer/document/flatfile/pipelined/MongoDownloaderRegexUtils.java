package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import com.mongodb.client.model.Filters;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.bson.conversions.Bson;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

class MongoDownloaderRegexUtils {
    private static final Logger LOG = LoggerFactory.getLogger(PipelinedMongoDownloadTask.class);

    final static Pattern LONG_PATH_ID_PATTERN = Pattern.compile("^[0-9]{1,3}:h");

    /**
     * Creates the filter to be used in the Mongo query
     *
     * @param mongoFilterPaths          The paths to be included/excluded in the filter. These define subtrees to be included or excluded.
     *                                  (see {@link MongoRegexPathFilterFactory.MongoFilterPaths} for details)
     * @param customExcludeEntriesRegex Documents with paths matching this regex are excluded from download
     * @return The filter to be used in the Mongo query, or null if no filter is required
     */
    static Bson computeMongoQueryFilter(@NotNull MongoRegexPathFilterFactory.MongoFilterPaths mongoFilterPaths, String customExcludeEntriesRegex) {
        var filters = new ArrayList<Bson>();

        List<Pattern> includedPatterns = compileIncludedDirectoriesRegex(mongoFilterPaths.included);
        if (!includedPatterns.isEmpty()) {
            // The conditions above on the _id field is not enough to match all JCR nodes in the given paths because nodes
            // with paths longer than a certain threshold, are represented by Mongo documents where the _id field is replaced
            // by a hash and the full path is stored in an additional field _path. To retrieve these long path documents,
            // we could add a condition on the _path field, but this would slow down substantially scanning the DB, because
            // the _path field is not part of the index used by this query (it's an index on _modified, _id). Therefore,
            // Mongo would have to retrieve every document from the column store to evaluate the filter condition. So instead
            // we add below a condition to download all the long path documents. These documents can be identified by the
            // format of the _id field (<n>:h<hash>), so it is possible to identify them using only the index.
            // This might download documents for nodes that are not in the included paths, but those documents will anyway
            // be filtered in the transform stage. And in most repositories, the number of long path documents is very small,
            // often there are none, so the extra documents downloaded will not slow down by much the download. However, the
            // performance gains of evaluating the filter of the query using only the index are very significant, especially
            // when the index requires only a small number of nodes.
            var patternsWithLongPathInclude = new ArrayList<>(includedPatterns);
            patternsWithLongPathInclude.add(LONG_PATH_ID_PATTERN);
            filters.add(Filters.in(NodeDocument.ID, patternsWithLongPathInclude));
        }

        // The Mongo filter returned here will download the top level path of each excluded subtree, which in theory
        // should be excluded. That is, if the tree /a/b/c is excluded, the filter will download /a/b/c but none of
        // its descendants.
        // This is done because excluding also the top level path would add extra complexity to the filter and
        // would not have any measurable impact on performance because it only downloads a few extra documents, one
        // for each excluded subtree. The transform stage will anyway filter out these paths.
        List<Pattern> filterPatterns = compileExcludedDirectoriesRegex(mongoFilterPaths.excluded);
        filterPatterns.forEach(p -> filters.add(Filters.regex(NodeDocument.ID, p)));

        Pattern customRegexExcludePattern = compileCustomExcludedPatterns(customExcludeEntriesRegex);
        if (customRegexExcludePattern != null) {
            filters.add(Filters.regex(NodeDocument.ID, customRegexExcludePattern));
        }

        if (filters.isEmpty()) {
            return null;
        } else if (filters.size() == 1) {
            return filters.get(0);
        } else {
            return Filters.and(filters);
        }
    }

    private static List<Pattern> compileIncludedDirectoriesRegex(List<String> includedPaths) {
        return compileDirectoryRegex(includedPaths, false);
    }

    private static List<Pattern> compileExcludedDirectoriesRegex(List<String> excludedPaths) {
        return compileDirectoryRegex(excludedPaths, true);
    }

    private static List<Pattern> compileDirectoryRegex(List<String> paths, boolean negate) {
        if (paths.isEmpty()) {
            return List.of();
        }
        if (paths.size() == 1 && paths.get(0).equals("/")) {
            return List.of();
        }
        ArrayList<Pattern> patterns = new ArrayList<>();
        for (String path : paths) {
            if (!path.endsWith("/")) {
                path = path + "/";
            }
            String regex = "^[0-9]{1,3}:" + Pattern.quote(path);
            Pattern pattern;
            if (negate) {
                pattern = compileExcludedDirectoryRegex(regex);
            } else {
                pattern = Pattern.compile(regex);
            }
            patterns.add(pattern);
        }
        return patterns;
    }

    static Pattern compileExcludedDirectoryRegex(String regex) {
        // https://stackoverflow.com/questions/1240275/how-to-negate-specific-word-in-regex
        return Pattern.compile("^(?!" + regex + ")");
    }

    static Pattern compileCustomExcludedPatterns(String customRegexPattern) {
        if (customRegexPattern == null || customRegexPattern.trim().isEmpty()) {
            LOG.info("Mongo custom regex is disabled");
            return null;
        } else {
            LOG.info("Excluding nodes with paths matching regex: {}", customRegexPattern);
            var negatedRegex = "^(?!.*(" + customRegexPattern + ")$)";
            return Pattern.compile(negatedRegex);
        }
    }

    /**
     * Returns all the ancestors paths of the given list of paths. That is, if the list is ["/a/b/c", "/a/b/d"],
     * this method will return ["/", "/a", "/a/b", "/a/b/c", "/a/b/d"]. Note that the paths on the input list are also
     * returned, even though they are not strictly ancestors of themselves.
     */
    static List<String> getAncestors(List<String> paths) {
        TreeSet<String> ancestors = new TreeSet<>();
        for (String child : paths) {
            String parent = child;
            while (true) {
                ancestors.add(parent);
                if (PathUtils.denotesRoot(parent)) {
                    break;
                }
                parent = PathUtils.getParentPath(parent);
            }
        }
        return new ArrayList<>(ancestors);
    }


    static Bson ancestorsFilter(List<String> paths) {
        List<String> parentFilters = getAncestors(paths).stream()
                .map(Utils::getIdFromPath)
                .collect(Collectors.toList());
        return Filters.in(NodeDocument.ID, parentFilters);
    }
}
