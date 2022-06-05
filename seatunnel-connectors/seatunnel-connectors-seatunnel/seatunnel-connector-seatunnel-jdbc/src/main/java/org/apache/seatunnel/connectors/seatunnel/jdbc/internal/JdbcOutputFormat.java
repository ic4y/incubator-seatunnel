/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal;

import com.google.common.annotations.VisibleForTesting;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcExecutionOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.xml.transform.Source;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A JDBC outputFormat that supports batching records before writing records to database.
 */

public class JdbcOutputFormat<In, JdbcExec extends JdbcBatchStatementExecutor<In>>
        implements Serializable
{

    protected final JdbcConnectionProvider connectionProvider;

    /**
     * A factory for creating {@link JdbcBatchStatementExecutor} instance.
     *
     * @param <T> The type of instance.
     */
    public interface StatementExecutorFactory<T extends JdbcBatchStatementExecutor<?>>
            extends Supplier<T>, Serializable {}

    ;

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(JdbcOutputFormat.class);

    private final JdbcExecutionOptions executionOptions;
    private final StatementExecutorFactory<JdbcExec> statementExecutorFactory;

    private transient JdbcExec jdbcStatementExecutor;
    private transient int batchCount = 0;
    private transient volatile boolean closed = false;

    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient volatile Exception flushException;

    public JdbcOutputFormat(
            @Nonnull JdbcConnectionProvider connectionProvider,
            @Nonnull JdbcExecutionOptions executionOptions,
            @Nonnull StatementExecutorFactory<JdbcExec> statementExecutorFactory)
    {
        this.connectionProvider = checkNotNull(connectionProvider);
        this.executionOptions = checkNotNull(executionOptions);
        this.statementExecutorFactory = checkNotNull(statementExecutorFactory);
    }

    /**
     * Connects to the target database and initializes the prepared statement.
     */

    public void open()
            throws IOException
    {
        try {
            //初始化连接
            connectionProvider.getOrEstablishConnection();
        }
        catch (Exception e) {
            throw new IOException("unable to open JDBC writer", e);
        }
        //获取拥有连接的执行器
        jdbcStatementExecutor = createAndOpenStatementExecutor(statementExecutorFactory);

        //若配置了刷新事件或数量刷新 需要定时线程去执行flush
        //TODO 实时任务需要校验，在exactlyOnce下IntervalMs、BatchSize 不能配置或者不生效
        //TODO 离线任务必须配置IntervalMs、BatchSize中任意一项，或者填充默认值
        if (executionOptions.getBatchIntervalMs() != 0 && executionOptions.getBatchSize() != 1) {
            this.scheduler =
                    Executors.newScheduledThreadPool(
                            1, runnable -> {
                                AtomicInteger cnt = new AtomicInteger(0);
                                Thread thread = new Thread(runnable);
                                thread.setDaemon(true);
                                thread.setName("jdbc-upsert-output-format" + "-" + cnt.incrementAndGet());
                                return thread;
                            });
            this.scheduledFuture =
                    this.scheduler.scheduleWithFixedDelay(
                            () -> {
                                synchronized (JdbcOutputFormat.this) {
                                    if (!closed) {
                                        try {
                                            flush();
                                        }
                                        catch (Exception e) {
                                            flushException = e;
                                        }
                                    }
                                }
                            },
                            executionOptions.getBatchIntervalMs(),
                            executionOptions.getBatchIntervalMs(),
                            TimeUnit.MILLISECONDS);
        }
    }

    private JdbcExec createAndOpenStatementExecutor(
            StatementExecutorFactory<JdbcExec> statementExecutorFactory)
            throws IOException
    {
        JdbcExec exec = statementExecutorFactory.get();
        try {
            exec.prepareStatements(connectionProvider.getConnection());
        }
        catch (SQLException e) {
            throw new IOException("unable to open JDBC writer", e);
        }
        return exec;
    }

    private void checkFlushException()
    {
        if (flushException != null) {
            throw new RuntimeException("Writing records to JDBC failed.", flushException);
        }
    }

    //对外调用写入数据到buffer
    public final synchronized void writeRecord(In record)
            throws IOException
    {
        checkFlushException();
        try {
            addToBatch(record);
            batchCount++;
            if (executionOptions.getBatchSize() > 0
                    && batchCount >= executionOptions.getBatchSize()) {
                flush();
            }
        }
        catch (Exception e) {
            throw new IOException("Writing records to JDBC failed.", e);
        }
    }

    protected void addToBatch(In record)
            throws SQLException
    {
        jdbcStatementExecutor.addToBatch(record);
    }

    public synchronized void flush()
            throws IOException
    {
        checkFlushException();

        for (int i = 0; i <= executionOptions.getMaxRetries(); i++) {
            try {
                attemptFlush();
                batchCount = 0;
                break;
            }
            catch (SQLException e) {
                LOG.error("JDBC executeBatch error, retry times = {}", i, e);
                if (i >= executionOptions.getMaxRetries()) {
                    throw new IOException(e);
                }
                try {
                    if (!connectionProvider.isConnectionValid()) {
                        updateExecutor(true);
                    }
                }
                catch (Exception exception) {
                    LOG.error(
                            "JDBC connection is not valid, and reestablish connection failed.",
                            exception);
                    throw new IOException("Reestablish JDBC connection failed", exception);
                }
                try {
                    Thread.sleep(1000 * i);
                }
                catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "unable to flush; interrupted while doing another attempt", e);
                }
            }
        }
    }

    protected void attemptFlush()
            throws SQLException
    {
        jdbcStatementExecutor.executeBatch();
    }

    /**
     * Executes prepared statement and closes all resources of this instance.
     */
    public synchronized void close()
    {
        if (!closed) {
            closed = true;

            if (this.scheduledFuture != null) {
                scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }

            if (batchCount > 0) {
                try {
                    flush();
                }
                catch (Exception e) {
                    LOG.warn("Writing records to JDBC failed.", e);
                    throw new RuntimeException("Writing records to JDBC failed.", e);
                }
            }

            try {
                if (jdbcStatementExecutor != null) {
                    jdbcStatementExecutor.closeStatements();
                }
            }
            catch (SQLException e) {
                LOG.warn("Close JDBC writer failed.", e);
            }
        }
        connectionProvider.closeConnection();
        checkFlushException();
    }

    public void updateExecutor(boolean reconnect)
            throws SQLException, ClassNotFoundException
    {
        jdbcStatementExecutor.closeStatements();
        jdbcStatementExecutor.prepareStatements(
                reconnect
                        ? connectionProvider.reestablishConnection()
                        : connectionProvider.getConnection());
    }

    /**
     * Returns configured {@code JdbcExecutionOptions}.
     */
    public JdbcExecutionOptions getExecutionOptions()
    {
        return executionOptions;
    }

    @VisibleForTesting
    public Connection getConnection()
    {
        return connectionProvider.getConnection();
    }
}
