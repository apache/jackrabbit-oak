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
package org.apache.jackrabbit.oak.plugins.document.rdb;

import static org.apache.jackrabbit.oak.plugins.document.rdb.RDBJDBCTools.closeResultSet;
import static org.apache.jackrabbit.oak.plugins.document.rdb.RDBJDBCTools.closeStatement;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import org.apache.jackrabbit.oak.commons.StringUtils;

public class RDBBlobStoreFriend {

    public static void storeBlock(RDBBlobStore ds, byte[] digest, int level, byte[] data) throws IOException {
        ds.storeBlock(digest, level, data);
    }

    public static byte[] readBlockFromBackend(RDBBlobStore ds, byte[] digest) throws Exception {
        return ds.readBlockFromBackend(digest);
    }

    public static void killMetaEntry(RDBBlobStore ds, byte[] digest) throws Exception {
        String id = StringUtils.convertBytesToHex(digest);
        Connection con = ds.ch.getRWConnection();
        PreparedStatement prepDelMeta = null;
        try {
            prepDelMeta = con.prepareStatement("delete from " + ds.tnMeta + " where ID = ?");
            prepDelMeta.setString(1, id);
            prepDelMeta.execute();
        } finally {
            closeStatement(prepDelMeta);
            con.commit();
            ds.ch.closeConnection(con);
        }
    }

    public static boolean isDataEntryPresent(RDBBlobStore ds, byte[] digest) throws Exception {
        String id = StringUtils.convertBytesToHex(digest);
        Connection con = ds.ch.getROConnection();
        PreparedStatement prep = null;
        ResultSet rs = null;
        try {
            prep = con.prepareStatement("select ID from " + ds.tnData + " where ID = ?");
            prep.setString(1, id);
            rs = prep.executeQuery();
            return rs.next();
        } finally {
            closeResultSet(rs);
            closeStatement(prep);
            con.commit();
            ds.ch.closeConnection(con);
        }
    }

    public static void deleteChunks(RDBBlobStore ds, List<String> chunkIds, long maxLastModifiedTime) throws Exception {
        ds.deleteChunks(chunkIds, maxLastModifiedTime);
    }
}
