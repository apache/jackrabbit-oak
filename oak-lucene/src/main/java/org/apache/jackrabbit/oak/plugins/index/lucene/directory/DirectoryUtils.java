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

package org.apache.jackrabbit.oak.plugins.index.lucene.directory;

import org.apache.lucene.store.Directory;

public class DirectoryUtils {
    /**
     * Get the file length in best effort basis.
     * @return actual fileLength. -1 if cannot determine
     */
    public static long getFileLength(Directory dir, String fileName){
        try{
            //Check for file presence otherwise internally it results in
            //an exception to be created
            if (dir.fileExists(fileName)) {
                return dir.fileLength(fileName);
            }
        } catch (Exception ignore){

        }
        return -1;
    }
}
