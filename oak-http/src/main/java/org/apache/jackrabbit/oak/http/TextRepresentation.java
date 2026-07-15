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
package org.apache.jackrabbit.oak.http;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.http.HttpServletResponse;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.tika.mime.MediaType;

import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

class TextRepresentation implements Representation {

    @Override
    public MediaType getType() {
        return MediaType.TEXT_PLAIN;
    }

    public void render(Tree tree, HttpServletResponse response)
            throws IOException {
        PrintWriter writer = startResponse(response);

        for (PropertyState property : tree.getProperties()) {
            writer.print(property.getName());
            writer.print(": ");
            if (property.isArray()) {
                for (String value : property.getValue(STRINGS)) {
                    writer.print(value);
                    writer.print(", ");
                }
            } else {
                writer.print(property.getValue(STRING));
            }
            writer.print('\n');
        }
        for (Tree child : tree.getChildren()) {
            writer.print(child.getName());
            writer.print(": <");
            writer.print(response.encodeRedirectURL(child.getName()));
            writer.print(">\n");
        }
    }

    public void render(PropertyState property, HttpServletResponse response)
            throws IOException {
        PrintWriter writer = startResponse(response);
        if (property.isArray()) {
            for (String value : property.getValue(Type.STRINGS)) {
                writer.print(value);
                writer.print('\n');
            }
        } else {
            writer.print(property.getValue(STRING));
        }
    }

    private PrintWriter startResponse(HttpServletResponse response)
            throws IOException {
        response.setContentType(MediaType.TEXT_PLAIN.toString());
        response.setCharacterEncoding("UTF-8");
        return response.getWriter();
    }

}
