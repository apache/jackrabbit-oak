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

package org.apache.jackrabbit.oak.segment.file.tar;

import java.io.IOException;
import java.util.UUID;

/**
 * A strategy for the recovery of segments.
 */
public interface TarRecovery {

    /**
     * Recover the data and meta-data of the given segment. The implementor of
     * this method might want to parse the content of the segment and generate
     * any metadata as needed. The result of the recovery process has to be
     * saved in the provided {@link TarWriter}.
     *
     * @param uuid   the identifier of the segment.
     * @param data   the raw data of the segment.
     * @param entryRecovery the destination of the recovered data.
     * @throws IOException if an I/O error occurs while recovering the data of
     *                     the segment.
     */
    void recoverEntry(UUID uuid, byte[] data, EntryRecovery entryRecovery) throws IOException;

}
