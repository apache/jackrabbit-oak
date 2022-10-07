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
package org.apache.jackrabbit.oak.run;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;
import joptsimple.util.PathConverter;
import joptsimple.util.PathProperties;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.StandardSystemProperty.FILE_SEPARATOR;

/**
 * Command to concurrently download blobs from an azure datastore using sas token authentication.
 * <p>
 * Blobs are stored in a specific folder following the datastore structure format.
 */
public class DataStoreCopyCommand implements Command {

    private static final Logger LOG = LoggerFactory.getLogger(DataStoreCopyCommand.class);

    private String sourceRepo;
    private String includePath;
    private Path fileIncludePath;
    private String sasToken;
    private String outDir;
    private int concurrency;

    @Override
    public void execute(String... args) throws Exception {
        parseCommandLineParams(args);

        Stream<String> ids = null;
        try (Downloader downloader = new Downloader(concurrency)) {
            if (fileIncludePath != null) {
                ids = Files.lines(fileIncludePath);
            } else {
                ids = Arrays.stream(includePath.split(";"));
            }

            long startNano = System.nanoTime();
            List<Downloader.ItemResponse> responses = downloader.download(ids.map(id -> {
                Downloader.Item item = new Downloader.Item();
                item.source = sourceRepo + "/" + id;
                if (sasToken != null) {
                    item.source += "?" + sasToken;
                }
               item.destination = getDestinationFromId(id);
                return item;
            }).collect(Collectors.toList()));
            double totalTimeSeconds = (double) (System.nanoTime() - startNano) / 1_000_000_000;

            Map<Boolean, List<Downloader.ItemResponse>> partitioned =
                    responses.stream().collect(Collectors.partitioningBy(ir -> ir.failed));

            List<Downloader.ItemResponse> success = partitioned.get(false);
            if (!success.isEmpty()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("The following blobs were successfully downloaded:");
                    success.forEach(s -> LOG.debug("{} [{}] downloaded in {} ms", s.item.source,
                            IOUtils.humanReadableByteCount(s.size), s.time));
                }

                long totalBytes = success.stream().mapToLong(s -> s.size).sum();
                LOG.info("Elapsed Time (Seconds): {}", totalTimeSeconds);
                LOG.info("Number of File Transfers: {}", success.size());
                LOG.info("Total Bytes Transferred: {}[{}]", totalBytes, IOUtils.humanReadableByteCount(totalBytes));
                if (totalBytes > 10_000_000) {
                    LOG.info("Speed (MB/sec): {}", ((double) totalBytes / (1024 * 1024)) / totalTimeSeconds);
                } else {
                    LOG.info("Speed (KB/sec): {}", ((double) totalBytes / (1024)) / totalTimeSeconds);
                }
            }

            List<Downloader.ItemResponse> failures = partitioned.get(true);
            if (!failures.isEmpty()) {
                LOG.error("The following blobs threw an error:");
                failures.forEach(f -> LOG.error(f.item.source, f.throwable));
                throw new IllegalStateException("Errors while downloading blobs");
            }
        } finally {
            if (ids != null) {
                ids.close();
            }
        }
    }

    protected String getDestinationFromId(String id) {
        // Rename the blob names to match expected datastore cache format (remove the "-" in the name)
        String blobName = id.replaceAll("-", "");
        if (id.length() < 6) {
            LOG.warn("Blob with name {} is less than 6 chars. Cannot create data folder structure. Storing in the root folder", blobName);
            return outDir + FILE_SEPARATOR.value() + blobName;
        } else {
            return outDir + FILE_SEPARATOR.value()
                    + blobName.substring(0, 2) + FILE_SEPARATOR.value() + blobName.substring(2, 4) + FILE_SEPARATOR.value()
                    + blobName.substring(4, 6) + FILE_SEPARATOR.value() + blobName;
        }
    }

    protected void parseCommandLineParams(String... args) {
        OptionParser parser = new OptionParser();

        // options available for get-blobs only
        OptionSpec<String> sourceRepoOpt = parser.accepts("source-repo", "The source repository url")
                .withRequiredArg().ofType(String.class).required();

        OptionSpecBuilder includePathBuilder = parser.accepts("include-path",
                "Include only these paths when copying (separated by semicolon)");
        OptionSpecBuilder fileIncludePathBuilder = parser.accepts("file-include-path",
                "Include only the paths specified in the file (separated by newline)");
        parser.mutuallyExclusive(includePathBuilder, fileIncludePathBuilder);
        OptionSpec<String> includePathOpt = includePathBuilder.withRequiredArg().ofType(String.class);
        OptionSpec<Path> fileIncludePathOpt = fileIncludePathBuilder.withRequiredArg()
                .withValuesConvertedBy(new PathConverter(PathProperties.FILE_EXISTING, PathProperties.READABLE));

        OptionSpec<String> sasTokenOpt = parser.accepts("sas-token", "The SAS token to access Azure Storage")
                .withRequiredArg().ofType(String.class);
        OptionSpec<String> outDirOpt = parser.accepts("out-dir",
                        "Path where to store the blobs. Otherwise, blobs will be stored in the current directory.")
                .withRequiredArg().ofType(String.class).defaultsTo(System.getProperty("user.dir") + FILE_SEPARATOR.value() + "blobs");
        OptionSpec<Integer> concurrencyOpt = parser.accepts("concurrency",
                        "Max number of concurrent requests that can occur (the default value is equal to 16 multiplied by the number of cores)")
                .withRequiredArg().ofType(Integer.class).defaultsTo(16 * Runtime.getRuntime().availableProcessors());

        OptionSet optionSet = parser.parse(args);

        this.sourceRepo = optionSet.valueOf(sourceRepoOpt);
        this.includePath = optionSet.valueOf(includePathOpt);
        this.fileIncludePath = optionSet.valueOf(fileIncludePathOpt);
        this.sasToken = optionSet.valueOf(sasTokenOpt);
        this.outDir = optionSet.valueOf(outDirOpt);
        this.concurrency = optionSet.valueOf(concurrencyOpt);
    }
}
