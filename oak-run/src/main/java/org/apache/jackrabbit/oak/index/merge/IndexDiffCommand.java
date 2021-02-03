package org.apache.jackrabbit.oak.index.merge;

import static java.util.Arrays.asList;

import org.apache.jackrabbit.oak.run.commons.Command;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class IndexDiffCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<String> customDirOption = parser
                .accepts("custom", "Path to index definition files (.json). " +
                        "List differences between out-of-the-box and customized indexes").withOptionalArg()
                .defaultsTo("");
        OptionSpec<String> mergeDirectoryOption = parser
                .accepts("merge", "Path to index definition files (.json). " +
                        "This is the existing indexes when merging").withOptionalArg()
                .defaultsTo("");
        OptionSpec<String> mergeAddFileOption = parser
                .accepts("merge-add", "Path to index definition file (.json) to merge. " +
                        "Adds the new out-of-the-box index to each index definition.").withOptionalArg()
                .defaultsTo("");
        OptionSpec<String> compareDirectoryOption = parser
                .accepts("compare", "Path to index definition files (.json). " +
                        "Compare index1 and index2").withOptionalArg()
                .defaultsTo("");
        OptionSpec<String> index1Option = parser
                .accepts("index1", "Index 1").withOptionalArg()
                .defaultsTo("");
        OptionSpec<String> index2Option = parser
                .accepts("index2", "Index 2").withOptionalArg()
                .defaultsTo("");
        OptionSpec<String> extractFileOption = parser
                .accepts("extract", "File from where to extract (an) index definition(s) (.json). " +
                        "This will extract index1, or all indexes").withOptionalArg()
                .defaultsTo("");
        OptionSpec<String> targetDirectoryOption = parser
                .accepts("target", "Target directory where to store results.")
                .withOptionalArg()
                .defaultsTo("");

        OptionSpec<?> helpSpec = parser.acceptsAll(
                asList("h", "?", "help"), "show help").forHelp();

        OptionSet options = parser.parse(args);
        String customDir = customDirOption.value(options);
        String mergeDirectory = mergeDirectoryOption.value(options);
        String compareDirectory = compareDirectoryOption.value(options);
        String extractFile = extractFileOption.value(options);
        String targetDirectory = targetDirectoryOption.value(options);
        if (options.has(helpSpec) || (customDir.isEmpty() &&
                mergeDirectory.isEmpty() &&
                compareDirectory.isEmpty() &&
                extractFile.isEmpty())) {
            parser.printHelpOn(System.out);
            return;
        }
        if (!customDir.isEmpty()) {
            System.out.println("Listing differences between out-of-the-box and customized indexes " +
                    "for directory \"" + customDir + "\"");
            System.out.println(IndexDiff.collectCustomizations(customDir));
        }
        if (!mergeDirectory.isEmpty()) {
            String mergeAdd = mergeAddFileOption.value(options);
            if (mergeAdd.isEmpty()) {
                parser.printHelpOn(System.out);
                return;
            }
            if (targetDirectory.isEmpty()) {
                System.out.println("Merging indexes " +
                        "for directory \"" +
                        mergeDirectory +
                        "\" with \"" + mergeAdd + "\"");
                System.out.println(IndexDiff.mergeIndexes(mergeDirectory, mergeAdd));
            } else {
                System.out.println("Merging index " +
                        "for \"" +
                        mergeDirectory +
                        "\" with \"" + mergeAdd + "\"");
                IndexDiff.mergeIndex(mergeDirectory, mergeAdd, targetDirectory);
            }
        }
        if (!compareDirectory.isEmpty()) {
            String index1 = index1Option.value(options);
            String index2 = index2Option.value(options);
            if (index1.isEmpty() || index2.isEmpty()) {
                parser.printHelpOn(System.out);
                return;
            }
            System.out.println("Comparing indexes " + index1 + " and " + index2 +
                    " for directory \"" +
                    compareDirectory + "\"");
            System.out.println(IndexDiff.compareIndexes(compareDirectory, index1, index2));
        }
        if (!extractFile.isEmpty()) {
            String index1 = index1Option.value(options);
            if (!index1.isEmpty()) {
                System.out.println("Extracting index " + index1 + " from \"" +
                        extractFile + "\"");
                System.out.println(IndexDiff.extract(extractFile, index1));
            } else if (!targetDirectory.isEmpty()) {
                System.out.println("Extracting indexes to \"" + targetDirectory + "\" from \"" +
                        extractFile + "\"");
                IndexDiff.extractAll(extractFile, targetDirectory);
            }
        }
    }

}
