package org.apache.jackrabbit.oak.run;

import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.run.commons.Utils;

import java.util.Locale;

import static java.util.Arrays.copyOfRange;
import static org.apache.jackrabbit.oak.run.AvailableElasticModes.MODES;

public final class Main {
    private Main() {
        // Prevent instantiation.
    }

    public static void main(String[] args) throws Exception {
        Utils.printProductInfo(
                args,
                Main.class.getResourceAsStream("/META-INF/maven/org.apache.jackrabbit/oak-run-elastic/pom.properties"));

        Command command = MODES.getCommand("help");

        if (args.length > 0) {
            command = MODES.getCommand(args[0].toLowerCase(Locale.ENGLISH));

            if (command == null) {
                command = MODES.getCommand("help");
            }

            args = copyOfRange(args, 1, args.length);
        }

        command.execute(args);
    }
}
