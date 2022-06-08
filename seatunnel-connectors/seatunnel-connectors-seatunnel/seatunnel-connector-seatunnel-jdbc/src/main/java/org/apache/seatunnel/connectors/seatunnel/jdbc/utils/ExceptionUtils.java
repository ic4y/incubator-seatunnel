package org.apache.seatunnel.connectors.seatunnel.jdbc.utils;

import java.io.IOException;

public class ExceptionUtils {
    public static void rethrow(Throwable t) {
        if (t instanceof Error) {
            throw (Error) t;
        } else if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        } else {
            throw new RuntimeException(t);
        }
    }

    /**
     * Re-throws the given {@code Throwable} in scenarios where the signatures allows only
     * IOExceptions (and RuntimeException and Error).
     *
     * <p>Throws this exception directly, if it is an IOException, a RuntimeException, or an Error.
     * Otherwise it wraps it in an IOException and throws it.
     *
     * @param t The Throwable to be thrown.
     */
    public static void rethrowIOException(Throwable t) throws IOException {
        if (t instanceof IOException) {
            throw (IOException) t;
        } else if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        } else if (t instanceof Error) {
            throw (Error) t;
        } else {
            throw new IOException(t.getMessage(), t);
        }
    }
}
