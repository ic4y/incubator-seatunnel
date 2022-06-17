package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcUtils.getTableNameAndFields;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcInputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MySqlTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MysqlJdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcConnectionOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSourceState;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Set;

import scala.Tuple2;

/**
 * @Author: Liuli
 * @Date: 2022/6/14 20:52
 */
@AutoService(SeaTunnelSource.class)
public class JdbcSource implements SeaTunnelSource<SeaTunnelRow, JdbcSourceSplit, JdbcSourceState> {
    protected static final Logger LOG = LoggerFactory.getLogger(JdbcSource.class);

    private SeaTunnelContext seaTunnelContext;
    private JdbcSourceOptions jdbcSourceOptions;
    private SeaTunnelRowTypeInfo typeInfo;

    private JdbcInputFormat inputFormat;
    private JdbcRowConverter jdbcRowConverter;
    private JdbcDialectTypeMapper jdbcDialectTypeMapper;
    private JdbcConnectionProvider jdbcConnectionProvider;

    String sql = "select * from type_number";

    @Override
    public String getPluginName() {
        return "jdbc";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {

        jdbcSourceOptions = new JdbcSourceOptions(pluginConfig);
        jdbcDialectTypeMapper = new MySqlTypeMapper();
        jdbcConnectionProvider = new SimpleJdbcConnectionProvider(jdbcSourceOptions.getJdbcConnectionOptions());
        jdbcRowConverter = new MysqlJdbcRowConverter();

        try {
            typeInfo = initTableField(jdbcConnectionProvider.getOrEstablishConnection());
        } catch (Exception e) {
            throw new PrepareFailException("jdbc", PluginType.SOURCE, e.getMessage());
        }
        inputFormat = new JdbcInputFormat(
            jdbcConnectionProvider,
            jdbcRowConverter,
            typeInfo,
            sql,
            0,
            true
        );
    }

    @Override
    public SeaTunnelContext getSeaTunnelContext() {
        return seaTunnelContext;
    }

    @Override
    public void setSeaTunnelContext(SeaTunnelContext seaTunnelContext) {
        this.seaTunnelContext = seaTunnelContext;
    }

    @Override
    public SeaTunnelRowTypeInfo getRowTypeInfo() {
        Connection conn;
        SeaTunnelRowTypeInfo seaTunnelRowTypeInfo = null;
        try {
            conn = jdbcConnectionProvider.getOrEstablishConnection();
            seaTunnelRowTypeInfo = initTableField(conn);
            System.out.println("=====>" + seaTunnelRowTypeInfo);
        } catch (Exception e) {
            //TODO 这个地方需要明确获取RowTypeInfo 是否需要结束程序
            LOG.warn("get row type info exception", e);
        }
        this.typeInfo = seaTunnelRowTypeInfo;
        return seaTunnelRowTypeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, JdbcSourceSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new JdbcSourceReader(inputFormat, readerContext);
    }

    @Override
    public SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> createEnumerator(SourceSplitEnumerator.Context<JdbcSourceSplit> enumeratorContext) throws Exception {
        return new JdbcSourceSplitEnumerator(enumeratorContext);
    }

    @Override
    public SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> restoreEnumerator(SourceSplitEnumerator.Context<JdbcSourceSplit> enumeratorContext, JdbcSourceState checkpointState) throws Exception {
        return new JdbcSourceSplitEnumerator(enumeratorContext);
    }

    @Override
    public Serializer<JdbcSourceState> getEnumeratorStateSerializer() {
        return new DefaultSerializer<>();
    }

    private SeaTunnelRowTypeInfo initTableField(Connection conn) {
        ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
        ArrayList<String> fieldNames = new ArrayList<>();

        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSetMetaData resultSetMetaData = ps.getMetaData();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                fieldNames.add(resultSetMetaData.getColumnName(i));
                seaTunnelDataTypes.add(jdbcDialectTypeMapper.mapping(resultSetMetaData, i));
            }
        } catch (Exception e) {
            LOG.warn("get row type info exception", e);
        }
        return new SeaTunnelRowTypeInfo(fieldNames.toArray(new String[fieldNames.size()]), seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[seaTunnelDataTypes.size()]));
    }

}
