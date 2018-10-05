/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.solr.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import javax.jcr.Session;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeIterator;

/**
 * Utility class for generating a Solr synonyms file for expanding {@link javax.jcr.nodetype.NodeType}s
 */
public class NodeTypeIndexingUtils {

    public static File createPrimaryTypeSynonymsFile(String path, Session session) throws Exception {
        File file = new File(path);

        StringWriter stringWriter = new StringWriter();
        NodeTypeIterator allNodeTypes = session.getWorkspace().getNodeTypeManager().getAllNodeTypes();
        while (allNodeTypes.hasNext()) {
            NodeType nodeType = allNodeTypes.nextNodeType();
            NodeType[] superTypes = nodeType.getSupertypes();
            if (superTypes != null && superTypes.length > 0) {
                stringWriter.append(nodeType.getName()).append(" => ");
                for (int i = 0; i < superTypes.length; i++) {
                    stringWriter.append(superTypes[i].getName());
                    if (i < superTypes.length - 1) {
                        stringWriter.append(',');
                    }
                    stringWriter.append(' ');
                }
                stringWriter.append('\n');
            }
        }
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        fileOutputStream.write(stringWriter.toString().getBytes("UTF-8"));
        fileOutputStream.flush();
        fileOutputStream.close();

        if (file.exists() || file.createNewFile()) {
            return file;
        } else {
            throw new IOException("primary types synonyms file could not be created");
        }
    }
}
