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

package org.apache.jackrabbit.oak.plugins.segment.file;

import java.io.DataInput;
import java.io.IOException;
import java.util.LinkedList;

import com.google.common.collect.Lists;

/**
 * Reader for journal files of the SegmentMK.
 */
public final class JournalReader {

    private JournalReader() { }

    /**
     * Read a journal file
     * @param journal  name of the journal file
     * @return  list of revisions listed in the journal. Oldest revision first.
     * @throws IOException
     */
    public static LinkedList<String> heads(DataInput journal) throws IOException {
        LinkedList<String> heads = Lists.newLinkedList();
        String line = journal.readLine();
        while (line != null) {
            int space = line.indexOf(' ');
            if (space != -1) {
                heads.add(line.substring(0, space));
            }
            line = journal.readLine();
        }
        return heads;
    }

}
