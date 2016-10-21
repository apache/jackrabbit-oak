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
package org.apache.jackrabbit.oak.upgrade.version;

import java.util.Calendar;

/**
 * This class allows to configure the behaviour of the version copier.
 */
public class VersionCopyConfiguration {

    private Calendar copyVersions;

    private Calendar copyOrphanedVersions;

    public VersionCopyConfiguration() {
        final Calendar epoch = Calendar.getInstance();
        epoch.setTimeInMillis(0);
        this.copyVersions = epoch;
        this.copyOrphanedVersions = epoch;
    }

    public void setCopyVersions(Calendar copyVersions) {
        this.copyVersions = copyVersions;
    }

    public void setCopyOrphanedVersions(Calendar copyOrphanedVersions) {
        this.copyOrphanedVersions = copyOrphanedVersions;
    }

    public Calendar getVersionsMinDate() {
        return copyVersions;
    }

    public Calendar getOrphanedMinDate() {
        if (copyVersions == null) {
            return copyVersions;
        } else if (copyOrphanedVersions != null && copyVersions.after(copyOrphanedVersions)) {
            return copyVersions;
        } else {
            return copyOrphanedVersions;
        }
    }

    public boolean isCopyVersions() {
        return copyVersions != null;
    }

    public boolean skipOrphanedVersionsCopy() {
        return copyVersions == null || copyOrphanedVersions == null;
    }

    public boolean isCopyAll() {
        return copyVersions != null && copyVersions.getTimeInMillis() == 0 && copyOrphanedVersions != null && copyOrphanedVersions.getTimeInMillis() == 0;
    }

}