package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import com.google.auto.service.AutoService;
import com.mysql.cj.jdbc.MysqlDataSource;
import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcStatementBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcConnectionOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcExactlyOnceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcExecutionOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.SerializableSupplier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.xa.JdbcExactlyOnceSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.xa.JdbcSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.xa.JdbcSinkCommitter;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import javax.sql.XADataSource;

import java.io.IOException;
import java.util.Optional;

/**
 * @Author: Liuli
 * @Date: 2022/5/30 23:08
 */
@AutoService(SeaTunnelSink.class)
public class JdbcSink implements SeaTunnelSink<SeaTunnelRow, JdbcSinkState, XidInfo, JdbcAggregatedCommitInfo>
{
    private  String sql;
    private  JdbcStatementBuilder<SeaTunnelRow> statementBuilder;
    private  JdbcConnectionProvider connectionProvider;
    private  JdbcExecutionOptions executionOptions;

    private Config pluginConfig;
    private SeaTunnelRowTypeInfo seaTunnelRowTypeInfo;
    private SeaTunnelContext seaTunnelContext;

    private   MySqlXaDataSourceFactory mySqlXaDataSourceFactory =      new MySqlXaDataSourceFactory("jdbc:mysql://localhost/test","root","123456");

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
        this.executionOptions = JdbcExecutionOptions.builder().withMaxRetries(0).build();
        this.sql = "insert into test_table(name,age) values(?,?)";
    }

    @Override
    public SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> createWriter(SinkWriter.Context context)
            throws IOException
    {
//        return  new JdbcSinkWriter(
//                sql,
//                (st, row) -> JdbcUtils.setRecordToStatement(st, null, row),
//                connectionProvider,
//                executionOptions);

        return new JdbcExactlyOnceSinkWriter(
                context,
                sql,
                (st, row) -> JdbcUtils.setRecordToStatement(st, null, row),
                executionOptions,
                JdbcExactlyOnceOptions.builder().build(),
                mySqlXaDataSourceFactory
        );

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
    public Optional<SinkCommitter<XidInfo>> createCommitter()
            throws IOException
    {
        JdbcExactlyOnceOptions.JDBCExactlyOnceOptionsBuilder builder = JdbcExactlyOnceOptions.builder();
        JdbcExactlyOnceOptions build = builder.build();
        System.out.println("---->SinkCommitter");
        return Optional.of(new JdbcSinkCommitter(build, mySqlXaDataSourceFactory));
    }

    @Override
    public Optional<SinkAggregatedCommitter<XidInfo, JdbcAggregatedCommitInfo>> createAggregatedCommitter()
            throws IOException
    {
        JdbcExactlyOnceOptions.JDBCExactlyOnceOptionsBuilder builder = JdbcExactlyOnceOptions.builder();
        JdbcExactlyOnceOptions build = builder.build();
        System.out.println("---->SinkAggregatedCommitter");
        return Optional.of(new JdbcSinkAggregatedCommitter(build, mySqlXaDataSourceFactory));
    }

    @Override
    public Optional<Serializer<JdbcAggregatedCommitInfo>> getAggregatedCommitInfoSerializer()
    {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public void setSeaTunnelContext(SeaTunnelContext seaTunnelContext) {
        this.seaTunnelContext = seaTunnelContext;
    }

    private static class MySqlXaDataSourceFactory
            implements SerializableSupplier<XADataSource>
    {
        private final String jdbcUrl;
        private final String username;
        private final String password;

        public MySqlXaDataSourceFactory(String jdbcUrl, String username, String password) {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
        }

        @Override
        public XADataSource get() {
            MysqlXADataSource xaDataSource = new MysqlXADataSource();
            xaDataSource.setUrl(jdbcUrl);
            xaDataSource.setUser(username);
            xaDataSource.setPassword(password);
            return xaDataSource;
        }
    }

    @Override
    public Optional<Serializer<XidInfo>> getCommitInfoSerializer()
    {
        return Optional.of(new DefaultSerializer<>());
    }
}
