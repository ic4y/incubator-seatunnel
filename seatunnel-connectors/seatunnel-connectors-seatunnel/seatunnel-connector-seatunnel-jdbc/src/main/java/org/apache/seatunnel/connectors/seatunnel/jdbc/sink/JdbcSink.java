package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcStatementBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcConnectionOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcExecutionOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcUtils;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @Author: Liuli
 * @Date: 2022/5/30 23:08
 */
@AutoService(SeaTunnelSink.class)
public class JdbcSink implements SeaTunnelSink<SeaTunnelRow, JdbcSinkState, JdbcCommitInfo, JdbcAggregatedCommitInfo>
{
    private  String sql;
    private  JdbcStatementBuilder<SeaTunnelRow> statementBuilder;
    private  JdbcConnectionProvider connectionProvider;
    private  JdbcExecutionOptions executionOptions;

    private Config pluginConfig;
    private SeaTunnelRowTypeInfo seaTunnelRowTypeInfo;
    private SeaTunnelContext seaTunnelContext;

    @Override
    public String getPluginName()
    {
        return "jdbc";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.pluginConfig = pluginConfig;
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder jdbcConnectionOptionsBuilder = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder();
        jdbcConnectionOptionsBuilder.withDriverName("com.mysql.cj.jdbc.Driver");
        jdbcConnectionOptionsBuilder.withUrl("jdbc:mysql://localhost/test");
        jdbcConnectionOptionsBuilder.withUsername("root");
        jdbcConnectionOptionsBuilder.withPassword("123456");
        this.connectionProvider = new SimpleJdbcConnectionProvider(jdbcConnectionOptionsBuilder.build());
        this.executionOptions = JdbcExecutionOptions.builder().build();
        this.sql = "insert into test_table(name,age) values(?,?)";
    }

    @Override
    public SinkWriter<SeaTunnelRow, JdbcCommitInfo, JdbcSinkState> createWriter(SinkWriter.Context context)
            throws IOException
    {

        try {
            return  new JdbcSinkWriter(sql, (st, row) -> JdbcUtils.setRecordToStatement(st, null, row), connectionProvider, executionOptions);
        }
        catch (Exception e) {
            e.printStackTrace();
           throw new RuntimeException("createWriter err :" + e.getMessage());
        }

    }

    @Override
    public void setTypeInfo(SeaTunnelRowTypeInfo seaTunnelRowTypeInfo) {
        this.seaTunnelRowTypeInfo = seaTunnelRowTypeInfo;
    }

    @Override
    public SeaTunnelContext getSeaTunnelContext() {
        return seaTunnelContext;
    }

    @Override
    public void setSeaTunnelContext(SeaTunnelContext seaTunnelContext) {
        this.seaTunnelContext = seaTunnelContext;
    }
}
