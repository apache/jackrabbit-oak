package org.apache.jackrabbit.oak.run;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.index.ElasticIndexCommand;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.run.commons.Modes;

public final class AvailableElasticModes {
    // list of available Modes for the tool
    public static final Modes MODES = new Modes(
            ImmutableMap.<String, Command>builder()
                    .put("index", new ElasticIndexCommand())
                    .build());
}
