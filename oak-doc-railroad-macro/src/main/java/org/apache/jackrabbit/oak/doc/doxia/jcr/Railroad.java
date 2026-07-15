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
package org.apache.jackrabbit.oak.doc.doxia.jcr;

import org.h2.bnf.Bnf;
import org.h2.server.web.PageParser;
import org.h2.tools.Csv;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * JCR 2.0 / SQL-2 railroad generator.
 */
public class Railroad {

    private final Map<String, String> map;

    public Railroad(String fileName) throws Exception {
        Bnf bnf = Bnf.getInstance(getReader(fileName));
        Csv csv = new Csv();
        csv.setLineCommentCharacter('#');
        ResultSet rs = csv.read(getReader(fileName), null);

        map = map(bnf, rs);
    }

    public String render(String topic) {
        return map.get(topic);
    }

    public static String getLink(String topic) {
        String link = topic.toLowerCase();
        link = link.replace(' ', '_');
        // link = StringUtils.replaceAll(link, "_", "");
        link = link.replace('@', '_');

        return link;
    }

    private Map<String, String> map(Bnf bnf, ResultSet rs) throws Exception {
        Map<String, String> map = new LinkedHashMap<String, String>();
        try {
            while (rs.next()) {
                ResultSetMetaData meta = rs.getMetaData();
                for (int i = 0; i < meta.getColumnCount(); i++) {
                    String k = StringUtils.toLowerEnglish(meta.getColumnLabel(i + 1));
                    String value = rs.getString(i + 1);
                    value = value.trim();
                    map.put(k, PageParser.escapeHtml(value));
                }
                String topic = rs.getString("TOPIC");
                String syntax = rs.getString("SYNTAX").trim();

                BnfRailroad r = new BnfRailroad();
                String railroad = r.getHtml(bnf, syntax);
                map.put(topic, railroad);
            }
        } finally {
            IOUtils.closeSilently(rs);
        }

        return map;
    }

    private static Reader getReader(String fileName) throws IOException {
        return new FileReader(new File(fileName));
    }
}
