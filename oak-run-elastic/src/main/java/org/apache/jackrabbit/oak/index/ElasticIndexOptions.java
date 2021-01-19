package org.apache.jackrabbit.oak.index;

import joptsimple.OptionParser;
import joptsimple.OptionSpec;
import org.apache.jackrabbit.oak.run.cli.OptionsBeanFactory;

public class ElasticIndexOptions extends IndexOptions {

    public static final OptionsBeanFactory FACTORY = ElasticIndexOptions::new;

    private final OptionSpec<String> scheme;
    private final OptionSpec<String> host;
    private final OptionSpec<String> apiKeyId;
    private final OptionSpec<String> apiKeySecret;
    private final OptionSpec<Integer> port;
    private final OptionSpec<String> indexPrefix;


    public ElasticIndexOptions(OptionParser parser) {
        super(parser);
        scheme = parser.accepts("scheme", "Elastic scheme")
                .withRequiredArg().ofType(String.class);
        host = parser.accepts("host", "Elastic host")
                .withRequiredArg().ofType(String.class);
        port = parser.accepts("port", "Elastic port")
                .withRequiredArg().ofType(Integer.class);
        apiKeyId = parser.accepts("apiKeyId", "Elastic api key id")
                .withRequiredArg().ofType(String.class);
        apiKeySecret = parser.accepts("apiKeySecret", "Elastic api key host")
                .withRequiredArg().ofType(String.class);
        indexPrefix = parser.accepts("indexPrefix", "Elastic indexPrefix")
                .withRequiredArg().ofType(String.class);
    }

    public String getElasticScheme() {
        return scheme.value(options);
    }

    public int getElasticPort() {
        return port.value(options);
    }

    public String getElasticHost() {
        return host.value(options);
    }

    public String getApiKeyId() {
        return apiKeyId.value(options);
    }

    public String getApiKeySecret() {
        return apiKeySecret.value(options);
    }

    public String getIndexPrefix() {
        return indexPrefix.value(options);
    }
}
