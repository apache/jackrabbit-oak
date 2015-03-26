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
package org.apache.jackrabbit.oak.jcr.xml;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import javax.jcr.ImportUUIDBehavior;
import javax.jcr.Node;

import org.apache.jackrabbit.test.AbstractJCRTest;

public class ImportTest extends AbstractJCRTest {

    public void testReplaceUUID() throws Exception {
        Node node = testRootNode.addNode(nodeName1);
        node.addMixin(mixReferenceable);
        superuser.save();

        OutputStream out = new ByteArrayOutputStream();
        superuser.exportSystemView(node.getPath(), out, true, false);

        superuser.importXML(node.getPath(), new ByteArrayInputStream(out.toString().getBytes()), ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING);
        superuser.save();

        assertTrue(testRootNode.hasNode(nodeName1));
        Node n2 = testRootNode.getNode(nodeName1);
        assertTrue(n2.isNodeType(mixReferenceable));
        assertFalse(n2.hasNode(nodeName1));
    }

//  TODO : FIXME (OAK-2246)
//    public void testTransientReplaceUUID() throws Exception {
//        Node node = testRootNode.addNode(nodeName1);
//        node.addMixin(mixReferenceable);
//
//        OutputStream out = new ByteArrayOutputStream();
//        superuser.exportSystemView(node.getPath(), out, true, false);
//
//        superuser.importXML(node.getPath(), new ByteArrayInputStream(out.toString().getBytes()), ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING);
//        superuser.save();
//
//        assertTrue(testRootNode.hasNode(nodeName1));
//        Node n2 = testRootNode.getNode(nodeName1);
//        assertTrue(n2.isNodeType(mixReferenceable));
//        assertFalse(n2.hasNode(nodeName1));
//    }
}