package org.apache.seatunnel.connectors.seatunnel.jdbc.utils;

/**
 * @Author: Liuli
 * @Date: 2022/5/30 22:38
 */
public class ExceptionUtils
{
    public static void rethrow(Throwable t) {
        if (t instanceof Error) {
            throw (Error) t;
        } else if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        } else {
            throw new RuntimeException(t);
        }
    }
}
