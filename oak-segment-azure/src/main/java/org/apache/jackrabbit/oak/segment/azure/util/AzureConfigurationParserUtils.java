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
package org.apache.jackrabbit.oak.segment.azure.util;

import static org.apache.jackrabbit.oak.segment.azure.util.AzureConfigurationParserUtils.AzureConnectionKey.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for parsing Oak Segment Azure configuration (e.g. connection
 * string, container name, uri, etc.) from custom encoded String or Azure
 * standard URI.
 */
public class AzureConfigurationParserUtils {
    public enum AzureConnectionKey {
        DEFAULT_ENDPOINTS_PROTOCOL("DefaultEndpointsProtocol"),
        ACCOUNT_NAME("AccountName"),
        ACCOUNT_KEY("AccountKey"),
        BLOB_ENDPOINT("BlobEndpoint"),
        CONTAINER_NAME("ContainerName"),
        DIRECTORY("Directory");

        private String text;

        AzureConnectionKey(String text) {
            this.text = text;
        }

        public String text() {
            return text;
        }
    }

    public static final String KEY_CONNECTION_STRING = "connectionString";
    public static final String KEY_CONTAINER_NAME = "containerName";
    public static final String KEY_ACCOUNT_NAME = "accountName";
    public static final String KEY_STORAGE_URI = "storageUri";
    public static final String KEY_DIR = "directory";

    private AzureConfigurationParserUtils() {
        // prevent instantiation
    }

    /**
     *
     * @param conn
     *            the connection string
     * @return <code>true</code> if this is a custom encoded Azure connection
     *         String, <code>false</code> otherwise
     */
    public static boolean isCustomAzureConnectionString(String conn) {
        return conn.contains(DEFAULT_ENDPOINTS_PROTOCOL.text());
    }

    /**
     * Parses a custom encoded connection string of the form (line breaks added for
     * clarity):
     * <br><br>
     * <b>DefaultEndpointsProtocol</b>=http;<b>AccountName</b>=devstoreaccount1;
     * <b>AccountKey</b>=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;
     * <b>BlobEndpoint</b>=http://127.0.0.1:10000/devstoreaccount1;<br>
     * <b>ContainerName</b>=mycontainer;<br>
     * <b>Directory</b>=mydir
     * <br><br>
     * where the first three lines in the string represent a standard Azure
     * Connection String and the last two lines are Oak Segment Azure specific
     * arguments. Please note that all configuration keys are semicolon separated, except for the last entry. The order
     * of keys is not important.
     *
     * @param conn
     *            the connection string
     * @return parsed configuration map containing the Azure <b>connectionString</b>,
     *         <b>containerName</b> and <b>dir</b> (key names in bold)
     */
    public static Map<String, String> parseAzureConfigurationFromCustomConnection(String conn) {
        Map<AzureConnectionKey, String> tempConfig = new HashMap<>();

        String[] connKeys = conn.split(";");
        for (AzureConnectionKey key : AzureConnectionKey.values()) {
            for (String connKey : connKeys) {
                if (connKey.toLowerCase().startsWith(key.text().toLowerCase())) {
                    tempConfig.put(key, connKey.substring(connKey.indexOf("=") + 1));
                }
            }
        }

        StringBuilder connectionString = new StringBuilder();
        connectionString.append(DEFAULT_ENDPOINTS_PROTOCOL.text()).append("=").append(tempConfig.get(DEFAULT_ENDPOINTS_PROTOCOL)).append(";");
        connectionString.append(ACCOUNT_NAME.text()).append("=").append(tempConfig.get(ACCOUNT_NAME)).append(";");
        connectionString.append(ACCOUNT_KEY.text()).append("=").append(tempConfig.get(ACCOUNT_KEY)).append(";");
        connectionString.append(BLOB_ENDPOINT.text()).append("=").append(tempConfig.get(BLOB_ENDPOINT)).append(";");

        Map<String, String> config = new HashMap<>();
        config.put(KEY_CONNECTION_STRING, connectionString.toString());
        config.put(KEY_CONTAINER_NAME, tempConfig.get(CONTAINER_NAME));
        config.put(KEY_DIR, tempConfig.get(DIRECTORY));
        return config;
    }

    /**
     * Parses a standard Azure URI in the format
     * <b>https</b>://<b>myaccount</b>.blob.core.windows.net/<b>container</b>/<b>repo</b>,
     *
     * @param uriStr
     *            the Azure URI as string
     * @return parsed configuration map containing <b>accountName</b>, <b>storageUri</b> and <b>dir</b>
     * (key names in bold)
     */
    public static Map<String, String> parseAzureConfigurationFromUri(String uriStr) {
        Map<String, String> config = new HashMap<>();

        URI uri = null;
        try {
            uri = new URI(uriStr);
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }

        String host = uri.getHost();
        String path = uri.getPath();
        String scheme = uri.getScheme();

        int lastSlashPosPath = path.lastIndexOf('/');
        int dotPosHost = host.indexOf(".");

        String accountName = host.substring(0, dotPosHost);
        String container = path.substring(0, lastSlashPosPath);
        String storageUri = scheme + "://" + host + container;
        String dir = path.substring(lastSlashPosPath + 1);

        config.put(KEY_ACCOUNT_NAME, accountName);
        config.put(KEY_STORAGE_URI, storageUri);
        config.put(KEY_DIR, dir);

        return config;
    }
}

