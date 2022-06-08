package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcStatementBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcExecutionOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.xa.XidImpl;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * @Author: Liuli
 * @Date: 2022/5/30 23:16
 */
public class JdbcSinkWriter implements SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState>
{
    @Override
    public List<JdbcSinkState> snapshotState()
            throws IOException
    {
        System.out.println("------------------>snapshotState");
        return Arrays.asList(new JdbcSinkState(new XidImpl(123,"cc".getBytes(),"bb".getBytes())));
    }

    private final JdbcOutputFormat<SeaTunnelRow, JdbcBatchStatementExecutor<SeaTunnelRow>> outputFormat;


    public JdbcSinkWriter(
            JdbcStatementBuilder<SeaTunnelRow> statementBuilder,
            JdbcConnectorOptions jdbcConnectorOptions)
            throws IOException
    {

        JdbcConnectionProvider connectionProvider = new SimpleJdbcConnectionProvider(jdbcConnectorOptions);

        this.outputFormat = new JdbcOutputFormat<SeaTunnelRow, JdbcBatchStatementExecutor<SeaTunnelRow>>(
                connectionProvider,
                jdbcConnectorOptions,
                () -> JdbcBatchStatementExecutor.simple(jdbcConnectorOptions.getQuery(), statementBuilder, Function.identity()));
        outputFormat.open();

    }

    @Override
    public void write(SeaTunnelRow element)
            throws IOException
    {
        outputFormat.writeRecord(element);
    }

    @Override
    public Optional<XidInfo> prepareCommit()
            throws IOException
    {
        System.out.println("------------------>prepareCommit");
        outputFormat.flush();
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
        outputFormat.close();
    }
}
