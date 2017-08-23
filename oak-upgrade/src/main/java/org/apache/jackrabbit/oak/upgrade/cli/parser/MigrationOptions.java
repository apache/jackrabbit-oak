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
package org.apache.jackrabbit.oak.upgrade.cli.parser;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.commons.lang.StringUtils;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrationOptions {

    private static final Logger log = LoggerFactory.getLogger(MigrationOptions.class);

    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    private static final boolean ADD_SECONDARY_METADATA = Boolean.getBoolean("oak.upgrade.addSecondaryMetadata");

    private final boolean copyBinaries;

    private final boolean disableMmap;

    private final int cacheSizeInMB;

    private final Calendar copyVersions;

    private final Calendar copyOrphanedVersions;

    private final String[] includePaths;

    private final String[] excludePaths;

    private final String[] fragmentPaths;

    private final String[] excludeFragments;

    private final String[] mergePaths;

    private final boolean includeIndex;

    private final boolean failOnError;

    private final boolean earlyShutdown;

    private final boolean skipInitialization;

    private final boolean skipNameCheck;

    private final boolean ignoreMissingBinaries;

    private final boolean verify;

    private final boolean onlyVerify;

    private final boolean skipCheckpoints;

    private final boolean forceCheckpoints;

    private final String srcUser;

    private final String srcPassword;

    private final String dstUser;

    private final String dstPassword;

    private final String srcFbs;

    private final String srcFds;

    private final String srcS3Config;

    private final String srcS3;

    private final String dstFbs;

    private final String dstFds;

    private final String dstS3Config;

    private final String dstS3;

    private final Boolean srcExternalBlobs;

    public MigrationOptions(MigrationCliArguments args) throws CliArgumentException {
        this.disableMmap = args.hasOption(OptionParserFactory.DISABLE_MMAP);
        this.copyBinaries = args.hasOption(OptionParserFactory.COPY_BINARIES);
        if (args.hasOption(OptionParserFactory.CACHE_SIZE)) {
            this.cacheSizeInMB = args.getIntOption(OptionParserFactory.CACHE_SIZE);
        } else {
            this.cacheSizeInMB = 256;
        }

        final Calendar epoch = Calendar.getInstance();
        epoch.setTimeInMillis(0);
        if (args.hasOption(OptionParserFactory.COPY_VERSIONS)) {
            this.copyVersions = parseVersionCopyArgument(args.getOption(OptionParserFactory.COPY_VERSIONS));
        } else {
            this.copyVersions = epoch;
        }
        if (args.hasOption(OptionParserFactory.COPY_ORPHANED_VERSIONS)) {
            this.copyOrphanedVersions = parseVersionCopyArgument(args.getOption(OptionParserFactory.COPY_ORPHANED_VERSIONS));
        } else {
            this.copyOrphanedVersions = epoch;
        }
        this.includePaths = checkPaths(args.getOptionList(OptionParserFactory.INCLUDE_PATHS));
        this.excludePaths = checkPaths(args.getOptionList(OptionParserFactory.EXCLUDE_PATHS));
        this.fragmentPaths = checkPaths(args.getOptionList(OptionParserFactory.FRAGMENT_PATHS));
        this.excludeFragments = args.getOptionList(OptionParserFactory.EXCLUDE_FRAGMENTS);
        this.mergePaths = checkPaths(args.getOptionList(OptionParserFactory.MERGE_PATHS));
        this.includeIndex = args.hasOption(OptionParserFactory.INCLUDE_INDEX);
        this.failOnError = args.hasOption(OptionParserFactory.FAIL_ON_ERROR);
        this.earlyShutdown = args.hasOption(OptionParserFactory.EARLY_SHUTDOWN);
        this.skipInitialization = args.hasOption(OptionParserFactory.SKIP_INIT);
        this.skipNameCheck = args.hasOption(OptionParserFactory.SKIP_NAME_CHECK);
        this.ignoreMissingBinaries = args.hasOption(OptionParserFactory.IGNORE_MISSING_BINARIES);
        this.verify = args.hasOption(OptionParserFactory.VERIFY);
        this.onlyVerify = args.hasOption(OptionParserFactory.ONLY_VERIFY);
        this.skipCheckpoints = args.hasOption(OptionParserFactory.SKIP_CHECKPOINTS);
        this.forceCheckpoints = args.hasOption(OptionParserFactory.FORCE_CHECKPOINTS);

        this.srcUser = args.getOption(OptionParserFactory.SRC_USER);
        this.srcPassword = args.getOption(OptionParserFactory.SRC_USER);
        this.dstUser = args.getOption(OptionParserFactory.DST_USER);
        this.dstPassword = args.getOption(OptionParserFactory.DST_PASSWORD);

        this.srcFbs = args.getOption(OptionParserFactory.SRC_FBS);
        this.srcFds = args.getOption(OptionParserFactory.SRC_FDS);
        this.srcS3 = args.getOption(OptionParserFactory.SRC_S3);
        this.srcS3Config = args.getOption(OptionParserFactory.SRC_S3_CONFIG);

        this.dstFbs = args.getOption(OptionParserFactory.DST_FBS);
        this.dstFds = args.getOption(OptionParserFactory.DST_FDS);
        this.dstS3 = args.getOption(OptionParserFactory.DST_S3);
        this.dstS3Config = args.getOption(OptionParserFactory.DST_S3_CONFIG);

        if (args.hasOption(OptionParserFactory.SRC_EXTERNAL_BLOBS)) {
            this.srcExternalBlobs = args.getBooleanOption(OptionParserFactory.SRC_EXTERNAL_BLOBS);
        } else {
            this.srcExternalBlobs = null;
        }
    }

    public boolean isCopyBinaries() {
        return copyBinaries;
    }

    public boolean isDisableMmap() {
        return disableMmap;
    }

    public int getCacheSizeInMB() {
        return cacheSizeInMB;
    }

    public Calendar getCopyVersions() {
        return copyVersions;
    }

    public Calendar getCopyOrphanedVersions() {
        return copyOrphanedVersions;
    }

    public String[] getIncludePaths() {
        return includePaths;
    }

    public String[] getExcludePaths() {
        return excludePaths;
    }

    public String[] getFragmentPaths() {
        return fragmentPaths;
    }

    public String[] getExcludeFragments() {
        return excludeFragments;
    }

    public String[] getMergePaths() {
        return mergePaths;
    }

    public boolean isFailOnError() {
        return failOnError;
    }

    public boolean isEarlyShutdown() {
        return earlyShutdown;
    }

    public boolean isSkipInitialization() {
        return skipInitialization;
    }

    public boolean isSkipNameCheck() {
        return skipNameCheck;
    }

    public boolean isIncludeIndex() {
        return includeIndex;
    }

    public boolean isIgnoreMissingBinaries() {
        return ignoreMissingBinaries;
    }

    public boolean isVerify() {
        return verify;
    }

    public boolean isOnlyVerify() {
        return onlyVerify;
    }

    public boolean isSkipCheckpoints() {
        return skipCheckpoints;
    }

    public boolean isForceCheckpoints() {
        return forceCheckpoints;
    }

    public boolean isAddSecondaryMetadata() { return ADD_SECONDARY_METADATA; }

    public String getSrcUser() {
        return srcUser;
    }

    public String getSrcPassword() {
        return srcPassword;
    }

    public String getDstUser() {
        return dstUser;
    }

    public String getDstPassword() {
        return dstPassword;
    }

    public String getSrcFbs() {
        return srcFbs;
    }

    public String getSrcFds() {
        return srcFds;
    }

    public String getSrcS3Config() {
        return srcS3Config;
    }

    public String getSrcS3() {
        return srcS3;
    }

    public String getDstFbs() {
        return dstFbs;
    }

    public String getDstFds() {
        return dstFds;
    }

    public String getDstS3Config() {
        return dstS3Config;
    }

    public String getDstS3() {
        return dstS3;
    }

    public boolean isSrcFds() {
        return StringUtils.isNotBlank(srcFds);
    }

    public boolean isSrcFbs() {
        return StringUtils.isNotBlank(srcFbs);
    }

    public boolean isSrcS3() {
        return StringUtils.isNotBlank(srcS3) && StringUtils.isNotBlank(srcS3Config);
    }

    public boolean isDstFds() {
        return StringUtils.isNotBlank(dstFds);
    }

    public boolean isDstFbs() {
        return StringUtils.isNotBlank(dstFbs);
    }

    public boolean isDstS3() {
        return StringUtils.isNotBlank(dstS3) && StringUtils.isNotBlank(dstS3Config);
    }

    public boolean isSrcBlobStoreDefined() {
        return isSrcFbs() || isSrcFds() || isSrcS3();
    }

    public boolean isDstBlobStoreDefined() {
        return isDstFbs() || isDstFds() || isDstS3();
    }

    public void logOptions() {
        if (disableMmap) {
            log.info("Disabling memory mapped file access for Segment Store");
        }

        if (copyVersions == null) {
            log.info("copyVersions parameter set to false");
        } else {
            log.info("copyVersions parameter set to {}", DATE_FORMAT.format(copyVersions.getTime()));
        }

        if (copyOrphanedVersions == null) {
            log.info("copyOrphanedVersions parameter set to false");
        } else {
            log.info("copyOrphanedVersions parameter set to {}", DATE_FORMAT.format(copyOrphanedVersions.getTime()));
        }

        if (includePaths != null) {
            log.info("paths to include: {}", (Object) includePaths);
        }

        if (excludePaths != null) {
            log.info("paths to exclude: {}", (Object) excludePaths);
        }

        if (fragmentPaths != null) {
            log.info("paths supporting fragments: {}", (Object) fragmentPaths);
        }

        if (excludeFragments != null) {
            log.info("fragments to exclude: {}", (Object) excludeFragments);
        }

        if (failOnError) {
            log.info("Unreadable nodes will cause failure of the entire transaction");
        }

        if (earlyShutdown) {
            log.info("Source repository would be shutdown post copying of nodes");
        }

        if (skipInitialization) {
            log.info("The repository initialization will be skipped");
        }

        if (skipNameCheck) {
            log.info("Test for long-named nodes will be disabled");
        }

        if (includeIndex) {
            log.info("Index data for the paths {} will be copied", (Object) includePaths);
        }

        if (ignoreMissingBinaries) {
            log.info("Missing binaries won't break the migration");
        }

        if (srcExternalBlobs != null) {
            log.info("Source DataStore external blobs: {}", srcExternalBlobs);
        }

        if (skipCheckpoints) {
            log.info("Checkpoints won't be migrated");
        }

        if (forceCheckpoints) {
            log.info("Checkpoints will be migrated even with the custom paths specified");
        }

        if (ADD_SECONDARY_METADATA) {
            log.info("Secondary metadata will be added");
        }

        log.info("Cache size: {} MB", cacheSizeInMB);

    }

    private static Calendar parseVersionCopyArgument(String string) {
        final Calendar calendar;

        if (Boolean.parseBoolean(string)) {
            calendar = Calendar.getInstance();
            calendar.setTimeInMillis(0);
        } else if (string != null && string.matches("^\\d{4}-\\d{2}-\\d{2}$")) {
            calendar = Calendar.getInstance();
            try {
                calendar.setTime(DATE_FORMAT.parse(string));
            } catch (ParseException e) {
                return null;
            }
        } else {
            calendar = null;
        }
        return calendar;
    }

    public Boolean getSrcExternalBlobs() {
        return srcExternalBlobs;
    }

    private static String[] checkPaths(String[] paths) throws CliArgumentException {
        if (paths == null) {
            return paths;
        }
        for (String p : paths) {
            if (!PathUtils.isValid(p)) {
                throw new CliArgumentException("Following path is not valid: " + p, 1);
            }
        }
        return paths;
    }

}
