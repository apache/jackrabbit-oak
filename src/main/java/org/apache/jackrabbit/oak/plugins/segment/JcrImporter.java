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
package org.apache.jackrabbit.oak.plugins.segment;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class JcrImporter {

    private final SegmentWriter writer;

    public JcrImporter(SegmentWriter writer) {
        this.writer = writer;
    }

    public RecordId writeValue(Value value) throws RepositoryException {
        if (value.getType() == PropertyType.BINARY) {
            Binary binary = value.getBinary();
            try {
                return writer.writeStream(binary.getStream());
            } catch (IOException e) {
                throw new RepositoryException(
                        "Can not read a binary stream from the repository", e);
            } finally {
                binary.dispose();
            }
        } else {
            return writer.writeString(value.getString());
        }
    }

}
