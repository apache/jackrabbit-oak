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
import org.apache.jackrabbit.oak.run.commons.LoggingInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;

import static com.google.common.base.StandardSystemProperty.FILE_SEPARATOR;

/**
 * Command to concurrently download blobs from an azure datastore using sas token authentication.
 * <p>
 * Blobs are stored in a specific folder following the datastore structure format.
 */
public class DataStoreCopyCommand implements Command {

    private static final Logger LOG = LoggerFactory.getLogger(DataStoreCopyCommand.class);

    public static final String NAME = "datastore-copy";

    private String sourceRepo;
    private String includePath;
    private Path fileIncludePath;
    private String sasToken;
    private String outDir;
    private int concurrency;
    private int connectTimeout;
    private int readTimeout;
    private int maxRetries;
    private long retryInitialInterval;
    private boolean failOnError;
    private int slowLogThreshold;

    @Override
    public void execute(String... args) throws Exception {
        parseCommandLineParams(args);
        setupLogging();

        Stream<String> ids = null;
        try (Downloader downloader = new Downloader(concurrency, connectTimeout, readTimeout, maxRetries,
                retryInitialInterval, failOnError, slowLogThreshold)) {
            if (fileIncludePath != null) {
                ids = Files.lines(fileIncludePath);
            } else {
                ids = Arrays.stream(includePath.split(";"));
            }

            long startNano = System.nanoTime();

            ids.forEach(id -> {
                Downloader.Item item = new Downloader.Item();
                item.source = sourceRepo + "/" + id;
                if (sasToken != null) {
                    item.source += "?" + sasToken;
                }
                item.destination = getDestinationFromId(id);
                downloader.offer(item);
            });

            Downloader.DownloadReport report = downloader.waitUntilComplete();
            double totalTimeSeconds = (double) (System.nanoTime() - startNano) / 1_000_000_000;

            LOG.info("Elapsed Time (Seconds): {}", totalTimeSeconds);
            LOG.info("Number of File Transfers: {}", report.successes);
            LOG.info("Number of FAILED Transfers: {}", report.failures);
            LOG.info("Total Bytes Transferred: {}[{}]", report.totalBytesTransferred,
                    IOUtils.humanReadableByteCount(report.totalBytesTransferred));
            LOG.info("Speed (MB/sec): {}",
                    ((double) report.totalBytesTransferred / (1024 * 1024)) / totalTimeSeconds);

            // if failOnError=true the command already failed in case of errors. Here we are handling the failOnError=false case
            if (report.successes <= 0 && report.failures > 0) {
                LOG.error("No downloads succeeded. {} failures detected. Failing command", report.failures);
                throw new RuntimeException("Errors while downloading blobs");
            }
        } finally {
            if (ids != null) {
                ids.close();
            }
            shutdownLogging();
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

        OptionSpec<Integer> connectTimeoutOpt = parser.accepts("connect-timeout",
                        "Sets a specific timeout value, in milliseconds, to be used when opening a connection for a single blob (default 0, no timeout)")
                .withRequiredArg().ofType(Integer.class).defaultsTo(0);
        OptionSpec<Integer> readTimeoutOpt = parser.accepts("read-timeout",
                        "Sets the read timeout, in milliseconds when reading a single blob (default 0, no timeout)")
                .withRequiredArg().ofType(Integer.class).defaultsTo(0);
        OptionSpec<Integer> slowLogThresholdOpt = parser.accepts("slow-log-threshold",
                        "Threshold to log a WARN message for blobs taking considerable time (default 10_000ms[10s])")
                .withRequiredArg().ofType(Integer.class).defaultsTo(10_000);
        OptionSpec<Integer> maxRetriesOpt = parser.accepts("max-retries", "Max number of retries when a blob download fails (default 3)")
                .withRequiredArg().ofType(Integer.class).defaultsTo(3);
        OptionSpec<Long> retryInitialIntervalOpt = parser.accepts("retry-interval", "The initial retry interval in milliseconds (default 100)")
                .withRequiredArg().ofType(Long.class).defaultsTo(100L);
        OptionSpec<Boolean> failOnErrorOpt = parser.accepts("fail-on-error",
                        "If true fails the execution immediately after the first error, otherwise it continues processing all the blobs (default false)")
                .withRequiredArg().ofType(Boolean.class).defaultsTo(false);

        OptionSet optionSet = parser.parse(args);

        this.sourceRepo = optionSet.valueOf(sourceRepoOpt);
        this.includePath = optionSet.valueOf(includePathOpt);
        this.fileIncludePath = optionSet.valueOf(fileIncludePathOpt);
        this.sasToken = optionSet.valueOf(sasTokenOpt);
        this.outDir = optionSet.valueOf(outDirOpt);
        this.concurrency = optionSet.valueOf(concurrencyOpt);
        this.connectTimeout = optionSet.valueOf(connectTimeoutOpt);
        this.readTimeout = optionSet.valueOf(readTimeoutOpt);
        this.slowLogThreshold = optionSet.valueOf(slowLogThresholdOpt);
        this.maxRetries = optionSet.valueOf(maxRetriesOpt);
        this.retryInitialInterval = optionSet.valueOf(retryInitialIntervalOpt);
        this.fileIncludePath = optionSet.valueOf(fileIncludePathOpt);
        this.failOnError = optionSet.valueOf(failOnErrorOpt);
    }

    protected static void setupLogging() throws IOException {
        new LoggingInitializer(Files.createTempDirectory("oak-run_datastore-copy").toFile(), NAME, false).init();
    }

    private static void shutdownLogging() {
        LoggingInitializer.shutdownLogging();
    }
}
