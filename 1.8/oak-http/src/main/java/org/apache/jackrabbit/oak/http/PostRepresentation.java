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
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import javax.servlet.http.HttpServletResponse;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.tika.mime.MediaType;

import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

class PostRepresentation implements Representation {

    private static final MediaType TYPE =
            MediaType.parse("application/x-www-form-urlencoded");

    private static final String ENCODING = "UTF-8";

    @Override
    public MediaType getType() {
        return TYPE;
    }

    @Override
    public void render(Tree tree, HttpServletResponse response)
            throws IOException {
        PrintWriter writer = startResponse(response);

        boolean first = true;
        for (PropertyState property : tree.getProperties()) {
            String name = property.getName();
            if (property.isArray()) {
                for (String value : property.getValue(STRINGS)) {
                    first = render(first, name, value, writer);
                }
            } else {
                first = render(first, name, property.getValue(STRING), writer);
            }
        }
    }

    @Override
    public void render(PropertyState property, HttpServletResponse response)
            throws IOException {
        PrintWriter writer = startResponse(response);
        if (property.isArray()) {
            for (String value : property.getValue(STRINGS)) {
                render(value, writer);
                writer.print('\n');
            }
        } else {
            render(property.getValue(STRING), writer);
        }
    }

    private static PrintWriter startResponse(HttpServletResponse response)
            throws IOException {
        response.setContentType(TYPE.toString());
        response.setCharacterEncoding(ENCODING);
        return response.getWriter();
    }

    private static boolean render(
            boolean first, String name, String value, PrintWriter writer) {
        if (!first) {
            writer.print('&');
        }
        render(name, writer);
        writer.print('=');
        render(value, writer);
        return false;
    }

    private static void render(String string, PrintWriter writer) {
        try {
            writer.print(URLEncoder.encode(string, ENCODING));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

}
