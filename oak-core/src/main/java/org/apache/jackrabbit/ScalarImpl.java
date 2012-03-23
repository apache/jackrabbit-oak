package org.apache.jackrabbit;

import org.apache.jackrabbit.oak.api.Scalar;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.Callable;

public abstract class ScalarImpl implements Scalar {
    private final Type type;

    public static Scalar numberScalar(String value) {
        // todo improve
        try {
            return longScalar(Long.parseLong(value));
        }
        catch (NumberFormatException e) {
            return doubleScalar(Double.parseDouble(value));
        }
    }

    public static Scalar booleanScalar(final boolean value) {
        return value ? TRUE_SCALAR : FALSE_SCALAR;
    }
    
    public static Scalar longScalar(final long value) {
        return new LongScalar(value);
    }

    public static Scalar nullScalar() {
        return NULL_SCALAR;
    }
    
    public static Scalar doubleScalar(final double value) {
        return new DoubleScalar(value);
    }
    
    public static Scalar stringScalar(final String value) {
        if (value == null) {
            throw new IllegalArgumentException("Value must not be null");
        }
        return new StringScalar(value);
    }
    
    public static Scalar binaryScalar(final String value) {
        if (value == null) {
            throw new IllegalArgumentException("Value must not be null");
        }
        return new SmallBinaryScalar(value);
    }
    
    public static Scalar binaryScalar(final Callable<InputStream> valueProvider) {
        if (valueProvider == null) {
            throw new IllegalArgumentException("Value must not be null");
        }
        return new BinaryScalar(valueProvider);
    }

    private ScalarImpl(Type type) {
        this.type = type;
    }

    @Override
    public Type getType() {
        return type;   
    }

    @Override
    public boolean getBoolean() {
        return Boolean.valueOf(getString());
    }

    @Override
    public long getLong() {
        return Long.parseLong(getString());
    }

    @Override
    public double getDouble() {
        return Double.parseDouble(getString());
    }

    @Override
    public InputStream getInputStream() {
        try {
            return new ByteArrayInputStream(getString().getBytes("UTF-8"));
        }
        catch (UnsupportedEncodingException e) {
            // todo handle UnsupportedEncodingException
            return null;
        }
    }

    @Override
    public String toString() {
        return getString() + ": " + type;
    }

    //------------------------------------------------------------< private >---

    private static final BooleanScalar TRUE_SCALAR = new BooleanScalar(true);
    private static final BooleanScalar FALSE_SCALAR = new BooleanScalar(false);

    private static final class BooleanScalar extends ScalarImpl {
        private final boolean value;

        public BooleanScalar(boolean value) {
            super(Type.BOOLEAN);
            this.value = value;
        }

        @Override
        public boolean getBoolean() {
            return value;
        }

        @Override
        public String getString() {
            return Boolean.toString(value);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof Scalar)) {
                return false;
            }

            Scalar that = (Scalar) other;
            return that != null && that.getType() == Type.BOOLEAN && that.getBoolean() == value;
        }

        @Override
        public int hashCode() {
            return (value ? 1 : 0);
        }
    }

    private static final NullScalar NULL_SCALAR = new NullScalar();

    private static final class NullScalar extends ScalarImpl {
        protected NullScalar() {
            super(Type.NULL);
        }

        @Override
        public String getString() {
            return "null";
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof Scalar)) {
                return false;
            }

            Scalar that = (Scalar) other;
            return that != null && that.getType() == Type.NULL;
        }

        @Override
        public int hashCode() {
            return 42;
        }
    }

    private static final class LongScalar extends ScalarImpl {
        private final long value;

        public LongScalar(long value) {
            super(Type.LONG);
            this.value = value;
        }

        @Override
        public long getLong() {
            return value;
        }

        @Override
        public String getString() {
            return Long.toString(value);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof Scalar)) {
                return false;
            }

            Scalar that = (Scalar) other;
            return that != null && that.getType() == Type.LONG && that.getLong() == value;
        }

        @Override
        public int hashCode() {
            return (int) (value ^ (value >>> 32));
        }
    }

    private static final class DoubleScalar extends ScalarImpl {
        private final double value;

        public DoubleScalar(double value) {
            super(Type.DOUBLE);
            this.value = value;
        }

        @Override
        public double getDouble() {
            return value;
        }

        @Override
        public String getString() {
            return Double.toString(value);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof Scalar)) {
                return false;
            }

            Scalar that = (Scalar) other;
            return that != null && that.getType() == Type.DOUBLE && Double.compare(that.getDouble(), value) == 0;
        }

        @Override
        public int hashCode() {
            long h = value != 0.0d ? Double.doubleToLongBits(value) : 0L;
            return (int) (h ^ (h >>> 32));
        }
    }

    private static final class StringScalar extends ScalarImpl {
        private final String value;

        public StringScalar(String value) {
            super(Type.STRING);
            this.value = value;
        }

        @Override
        public String getString() {
            return value;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof Scalar)) {
                return false;
            }

            Scalar that = (Scalar) other;
            return that != null && that.getType() == Type.STRING && that.getString().equals(value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }

    private static final class SmallBinaryScalar extends ScalarImpl {
        private final String value;

        public SmallBinaryScalar(String value) {
            super(Type.BINARY);
            this.value = value;
        }

        @Override
        public String getString() {
            return value;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof Scalar)) {
                return false;
            }

            Scalar that = (Scalar) other;
            return that != null && that.getType() == Type.BINARY && that.getString().equals(value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }

    private static final class BinaryScalar extends ScalarImpl {
        private final Callable<InputStream> valueProvider;

        public BinaryScalar(Callable<InputStream> valueProvider) {
            super(Type.BINARY);
            this.valueProvider = valueProvider;
        }

        @Override
        public InputStream getInputStream() {
            try {
                return valueProvider.call();
            }
            catch (Exception e) {
                // todo handle Exception
                return null;
            }
        }

        @Override
        public String getString() {
            return ""; // todo implement getString
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof Scalar)) {
                return false;
            }

            Scalar that = (Scalar) other;
            return that != null && that.getType() == Type.BINARY && that.getString().equals(getString());
        }

        @Override
        public int hashCode() {
            return getString().hashCode();
        }
    }
     
}
