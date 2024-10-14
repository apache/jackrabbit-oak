package org.apache.jackrabbit.oak.commons.conditions;

import java.util.Objects;

import org.apache.jackrabbit.oak.commons.properties.SystemPropertySupplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Checks {

    private Checks() {
        // no instances for you
    }

    private static Logger LOG = LoggerFactory.getLogger(Checks.class);

    // when true, message template are arguments checked even when the condition
    // to check for is true
    private static boolean CHECKMESSAGETEMPLATE = SystemPropertySupplier
            .create("oak.precondition.checks.CHECKMESSAGETEMPLATE", false).loggingTo(LOG).get();

    /**
     * Checks the specified expression
     * 
     * @param expression
     *            to check
     * @throws IllegalArgumentException
     *             when false
     */
    public static void checkArgument(boolean expression) throws IllegalArgumentException {
        if (!expression) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Checks the specified expression
     * 
     * @param expression
     *            to check
     * @param message
     *            to use in exception
     * @throws IllegalArgumentException
     *             when false
     */
    public static void checkArgument(boolean expression, @NotNull String message) throws IllegalArgumentException {

        Objects.requireNonNull(message);

        if (!expression) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Checks the specified expression
     * 
     * @param expression
     *            to checks
     * @param messageTemplate
     *            to use in exception (using {@link String#format} syntax
     * @throws IllegalArgumentException
     *             when false
     */
    public static void checkArgument(boolean expression, @NotNull String messageTemplate, @Nullable Object... messageArgs) {

        Objects.requireNonNull(messageTemplate);

        if (CHECKMESSAGETEMPLATE) {
            checkTemplate(messageTemplate, messageArgs);
        }

        if (!expression) {
            if (!CHECKMESSAGETEMPLATE) {
                checkTemplate(messageTemplate, messageArgs);
            }

            String message = String.format(messageTemplate, messageArgs);
            throw new IllegalArgumentException(message);
        }
    }

    protected static boolean checkTemplate(@NotNull String messageTemplate, @Nullable Object... messageArgs) {
        int argsSpecified = messageArgs.length;
        int argsInTemplate = countArguments(messageTemplate);
        boolean result = argsSpecified == argsInTemplate;
        if (argsSpecified != argsInTemplate) {
            LOG.error("Invalid message format: template '{}', argument count {}", messageTemplate, argsSpecified);
        }
        return result;
    }

    protected static int countArguments(String template) {
        int count = 0;
        boolean inEscape = false;

        for (char c : template.toCharArray()) {
            if (inEscape) {
                if (c != '%') {
                    count += 1;
                }
                inEscape = false;
            } else {
                if (c == '%') {
                    inEscape = true;
                }
            }
        }

        if (inEscape) {
            LOG.error("trailing escape character '%' found", new Exception("call stack"));
        }

        return count;
    }
}
