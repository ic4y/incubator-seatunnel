package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcStatementBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcConnectionOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcExecutionOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.function.Function;

/**
 * @Author: Liuli
 * @Date: 2022/5/30 23:16
 */
public class JdbcSinkWriter implements SinkWriter<SeaTunnelRow, JdbcCommitInfo, JdbcSinkState>
{
    protected final JdbcConnectionProvider connectionProvider;
    protected final JdbcBatchStatementExecutor<SeaTunnelRow> jdbcBatchStatementExecutor;


    public JdbcSinkWriter(
            String sql,
            JdbcStatementBuilder<SeaTunnelRow> statementBuilder,
            JdbcConnectionProvider connectionProvider,
            JdbcExecutionOptions executionOptions)
            throws SQLException, ClassNotFoundException
    {
        this.connectionProvider = connectionProvider;
        this.jdbcBatchStatementExecutor = JdbcBatchStatementExecutor.simple(sql, statementBuilder, Function.identity());
        jdbcBatchStatementExecutor.prepareStatements(connectionProvider.getOrEstablishConnection());
    }

    @Override
    public void write(SeaTunnelRow element)
            throws IOException
    {
        try {
            jdbcBatchStatementExecutor.addToBatch(element);
            jdbcBatchStatementExecutor.executeBatch();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Optional<JdbcCommitInfo> prepareCommit()
            throws IOException
    {
        try {
            jdbcBatchStatementExecutor.executeBatch();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    @Override
    public void abort()
    {

    }

    @Override
    public void close()
            throws IOException
    {
        try {
            jdbcBatchStatementExecutor.closeStatements();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
