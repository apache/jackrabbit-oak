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
package org.apache.jackrabbit.mk.server;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

import org.apache.jackrabbit.oak.commons.IOUtils;

/**
 * File servlet that will deliver static resources.
 */
class FileServlet implements Servlet {
    
    /** The one and only instance of this servlet. */
    public static FileServlet INSTANCE = new FileServlet();
    
    /** Just one instance, no need to make constructor public */
    private FileServlet() {}

    @Override
    public void service(Request request, Response response) throws IOException {
        String file = request.getFile();
        if (file.endsWith("/")) {
            file += "index.html";
        }
        InputStream in = FileServlet.class.getResourceAsStream(file.substring(1));
        if (in != null) {
            try {
                int dotIndex = file.lastIndexOf('.');
                if (dotIndex != -1) {
                    String contentType = MIME_TYPES.get(file.substring(dotIndex + 1));
                    if (contentType == null) {
                        contentType = "application/octet-stream";
                    }
                    response.setContentType(contentType);
                }
                IOUtils.copy(in, response.getOutputStream());
            } finally {
                IOUtils.closeQuietly(in);
            }
        } else {
            response.setStatusCode(404);
        }
    }

    /* Mime types table */
    private static final HashMap<String, String> MIME_TYPES = new HashMap<String, String>();
    
    static {
        MIME_TYPES.put("html", "text/html");
        MIME_TYPES.put("css",  "text/css");
        MIME_TYPES.put("js",   "application/javascript");
        MIME_TYPES.put("json", "application/json");
        MIME_TYPES.put("png",  "image/png");
    }
}
