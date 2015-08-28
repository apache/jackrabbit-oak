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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrationOptions {

    private static final Logger log = LoggerFactory.getLogger(MigrationOptions.class);

    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    private final boolean copyBinariesByReference;

    private final boolean mmap;

    private final int cacheSizeInMB;

    private final boolean ldap;

    private final Calendar copyVersions;

    private final Calendar copyOrphanedVersions;

    private final String[] includePaths;

    private final String[] excludePaths;

    private final String[] mergePaths;

    private final boolean failOnError;

    private final boolean earlyShutdown;

    private final String[] disabledIndexes;

    public MigrationOptions(ArgumentParser args) {
        this.copyBinariesByReference = !args.hasOption(ArgumentParser.COPY_BINARIES);
        this.mmap = args.hasOption(ArgumentParser.MMAP);
        if (args.hasOption(ArgumentParser.CACHE_SIZE)) {
            this.cacheSizeInMB = args.getIntOption(ArgumentParser.CACHE_SIZE);
        } else {
            this.cacheSizeInMB = 256;
        }
        this.ldap = args.hasOption(ArgumentParser.LDAP);

        final Calendar epoch = Calendar.getInstance();
        epoch.setTimeInMillis(0);
        if (args.hasOption(ArgumentParser.COPY_VERSIONS)) {
            this.copyVersions = parseVersionCopyArgument(args.getOption(ArgumentParser.COPY_VERSIONS));
        } else {
            this.copyVersions = epoch;
        }
        if (args.hasOption(ArgumentParser.COPY_ORPHANED_VERSIONS)) {
            this.copyOrphanedVersions = parseVersionCopyArgument(args.getOption(ArgumentParser.COPY_ORPHANED_VERSIONS));
        } else {
            this.copyOrphanedVersions = epoch;
        }
        this.includePaths = split(args.getOption(ArgumentParser.INCLUDE_PATHS));
        this.excludePaths = split(args.getOption(ArgumentParser.EXCLUDE_PATHS));
        this.mergePaths = split(args.getOption(ArgumentParser.MERGE_PATHS));
        this.failOnError = args.hasOption(ArgumentParser.FAIL_ON_ERROR);
        this.earlyShutdown = args.hasOption(ArgumentParser.EARLY_SHUTDOWN);
        this.disabledIndexes = split(args.getOption(ArgumentParser.DISABLE_INDEXES));
        logOptions();
    }

    public boolean isCopyBinariesByReference() {
        return copyBinariesByReference;
    }

    public boolean isMmap() {
        return mmap;
    }

    public int getCacheSizeInMB() {
        return cacheSizeInMB;
    }

    public boolean isLdap() {
        return ldap;
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

    public String[] getMergePaths() {
        return mergePaths;
    }

    public boolean isFailOnError() {
        return failOnError;
    }

    public boolean isEarlyShutdown() {
        return earlyShutdown;
    }

    public String[] getDisabledIndexes() {
        return disabledIndexes;
    }

    private void logOptions() {
        if (copyBinariesByReference) {
            log.info("DataStore needs to be shared with new repository");
        } else {
            log.info("Binary content would be copied to the NodeStore.");
        }

        if (mmap) {
            log.info("Enabling memory mapped file access for Segment Store");
        }

        if (ldap) {
            log.info("rep:externalId properties will be created for LDAP principals");
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

        if (disabledIndexes != null) {
            log.info("Migration will disable following indexes {}.", (Object) disabledIndexes);
        }

        if (failOnError) {
            log.info("Unreadable nodes will cause failure of the entire transaction");
        }

        if (earlyShutdown) {
            log.info("Source repository would be shutdown post copying of nodes");
        }

        log.info("Cache size: {} MB", cacheSizeInMB);

    }

    private String[] split(String list) {
        if (list == null) {
            return null;
        } else {
            return list.split(",");
        }
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

}