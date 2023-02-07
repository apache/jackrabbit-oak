package org.apache.jackrabbit.oak.index;

import joptsimple.OptionParser;
import org.apache.jackrabbit.oak.indexversion.ElasticPurgeOldIndexVersion;
import org.apache.jackrabbit.oak.indexversion.PurgeOldIndexVersion;
import org.apache.jackrabbit.oak.run.PurgeOldIndexVersionCommand;
import org.apache.jackrabbit.oak.run.cli.CommonOptions;
import org.apache.jackrabbit.oak.run.cli.Options;

public class ElasticPurgeOldIndexVersionCommand extends PurgeOldIndexVersionCommand {

    private Options opts;
    private ElasticIndexOptions indexOpts;
    public static final String NAME = "purge-index-versions";
    private final String summary = "Provides elastic purge index related operations";

    @Override
    public void execute(String ... args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.allowsUnrecognizedOptions();

        opts = new Options();
        opts.setCommandName(NAME);
        opts.setSummary(summary);
        opts.setConnectionString(CommonOptions.DEFAULT_CONNECTION_STRING);
        opts.registerOptionsFactory(ElasticIndexOptions.FACTORY);
        opts.parseAndConfigure(parser, args);

        indexOpts = opts.getOptionBean(ElasticIndexOptions.class);
        super.execute(args);
    }

    @Override
    protected PurgeOldIndexVersion getPurgeOldIndexVersionImpl() {
        return new ElasticPurgeOldIndexVersion(indexOpts.getIndexPrefix(), indexOpts.getElasticScheme(), indexOpts.getElasticHost(),
                indexOpts.getElasticPort(), indexOpts.getApiKeyId(), indexOpts.getApiKeySecret());
    }
}
