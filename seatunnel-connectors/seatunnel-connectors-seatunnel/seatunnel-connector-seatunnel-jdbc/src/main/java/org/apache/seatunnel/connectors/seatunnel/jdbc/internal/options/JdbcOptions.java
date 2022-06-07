package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options;

/**
 * @Author: Liuli
 * @Date: 2022/6/7 18:14
 */
public class JdbcOptions
{

    public static final String URL = "url";

    public static final String DRIVER = "driver";

    public static final String CONNECTION_CHECK_TIMEOUT_SEC = "connection_check_timeout_sec";

    public static final String MAX_RETRIES = "max_retries";

    public static final String USER = "user";

    public static final String PASSWORD = "password";

    public static final String QUERY = "query";

    public static final String PARALLELISM = "parallelism";


    public static final String BATCH_SIZE = "batch_size";

    public static final String BATCH_INTERVAL_MS = "batch_interval_ms";


    public static final String IS_EXACTLY_ONCE = "is_exactly_once";

    public static final String XA_DATA_SOURCE_CLASS_NAME = "xa_data_source_class_name";


    public static final String MAX_COMMIT_ATTEMPTS = "max_commit_attempts";

    public static final String TRANSACTION_TIMEOUT_SEC = "transaction_timeout_sec";

}
