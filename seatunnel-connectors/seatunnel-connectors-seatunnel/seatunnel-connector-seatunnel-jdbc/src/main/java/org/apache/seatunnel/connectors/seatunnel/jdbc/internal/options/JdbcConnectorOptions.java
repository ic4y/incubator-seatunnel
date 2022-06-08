package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.Serializable;
import java.util.Optional;

public class JdbcConnectorOptions
    implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final int DEFAULT_CONNECTION_CHECK_TIMEOUT_SEC = 30;
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int DEFAULT_BATCH_SIZE = 300;
    private static final int DEFAULT_BATCH_INTERVAL_MS = 1000;
    private static final boolean DEFAULT_IS_EXACTLY_ONCE = false;
    private static final int DEFAULT_MAX_COMMIT_ATTEMPTS = 3;
    private static final int DEFAULT_TRANSACTION_TIMEOUT_SEC = -1;

    private final String url;
    private final String driverName;
    private int connectionCheckTimeoutSeconds = DEFAULT_CONNECTION_CHECK_TIMEOUT_SEC;
    private int maxRetries = DEFAULT_MAX_RETRIES;
    private final String username;
    private final String password;
    private final String query;

    private int batchSize = DEFAULT_BATCH_SIZE;
    private int batchIntervalMs = DEFAULT_BATCH_INTERVAL_MS;

    private boolean isExactlyOnce = DEFAULT_IS_EXACTLY_ONCE;
    private String xaDataSourceClassName;

    private int maxCommitAttempts = DEFAULT_MAX_COMMIT_ATTEMPTS;

    private int transactionTimeoutSec = DEFAULT_TRANSACTION_TIMEOUT_SEC;

    public JdbcConnectorOptions(Config config) {
        this.url = config.getString(JdbcConfig.URL);
        this.driverName = config.getString(JdbcConfig.DRIVER);
        this.username = config.getString(JdbcConfig.USER);
        this.password = config.getString(JdbcConfig.PASSWORD);
        this.query = config.getString(JdbcConfig.QUERY);

        if (config.hasPath(JdbcConfig.MAX_RETRIES)) {
            this.maxRetries = config.getInt(JdbcConfig.MAX_RETRIES);
        }
        if (config.hasPath(JdbcConfig.CONNECTION_CHECK_TIMEOUT_SEC)) {
            this.connectionCheckTimeoutSeconds = config.getInt(JdbcConfig.CONNECTION_CHECK_TIMEOUT_SEC);
        }
        if (config.hasPath(JdbcConfig.BATCH_SIZE)) {
            this.batchSize = config.getInt(JdbcConfig.BATCH_SIZE);
        }
        if (config.hasPath(JdbcConfig.BATCH_INTERVAL_MS)) {
            this.batchIntervalMs = config.getInt(JdbcConfig.BATCH_INTERVAL_MS);
        }

        if (config.hasPath(JdbcConfig.IS_EXACTLY_ONCE)) {
            this.isExactlyOnce = true;
            this.xaDataSourceClassName = config.getString(JdbcConfig.XA_DATA_SOURCE_CLASS_NAME);
            if (config.hasPath(JdbcConfig.MAX_COMMIT_ATTEMPTS)) {
                this.maxCommitAttempts = config.getInt(JdbcConfig.MAX_COMMIT_ATTEMPTS);
            }
            if (config.hasPath(JdbcConfig.TRANSACTION_TIMEOUT_SEC)) {
                this.transactionTimeoutSec = config.getInt(JdbcConfig.TRANSACTION_TIMEOUT_SEC);
            }
        }
    }

    public String getUrl() {
        return url;
    }

    public String getDriverName() {
        return driverName;
    }

    public int getConnectionCheckTimeoutSeconds() {
        return connectionCheckTimeoutSeconds;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getQuery() {
        return query;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getBatchIntervalMs() {
        return batchIntervalMs;
    }

    public boolean isExactlyOnce() {
        return isExactlyOnce;
    }

    public String getXaDataSourceClassName() {
        return xaDataSourceClassName;
    }

    public int getMaxCommitAttempts() {
        return maxCommitAttempts;
    }

    public Optional<Integer> getTransactionTimeoutSec() {
        return transactionTimeoutSec < 0 ? Optional.empty() : Optional.of(transactionTimeoutSec);
    }
}
