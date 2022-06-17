package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcUtils.extendPartitionQuerySql;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcUtils.getTableNameAndFields;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcInputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.split.JdbcNumericBetweenParametersProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.split.JdbcParameterValuesProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSourceState;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import scala.Tuple2;

@AutoService(SeaTunnelSource.class)
public class JdbcSource implements SeaTunnelSource<SeaTunnelRow, JdbcSourceSplit, JdbcSourceState> {
    protected static final Logger LOG = LoggerFactory.getLogger(JdbcSource.class);

    private SeaTunnelContext seaTunnelContext;
    private JdbcSourceOptions jdbcSourceOptions;
    private SeaTunnelRowTypeInfo typeInfo;

    private JdbcDialect jdbcDialect;
    private JdbcInputFormat inputFormat;
    private JdbcConnectionProvider jdbcConnectionProvider;

    private String query;
    private String tableName;
    private int parallelism;

    @Override
    public String getPluginName() {
        return "jdbc";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {

        jdbcSourceOptions = new JdbcSourceOptions(pluginConfig);
        jdbcConnectionProvider = new SimpleJdbcConnectionProvider(jdbcSourceOptions.getJdbcConnectionOptions());
        query = jdbcSourceOptions.getJdbcConnectionOptions().query;
        parallelism = jdbcSourceOptions.getParallelism().orElse(1);
        jdbcDialect = JdbcDialectLoader.load(jdbcSourceOptions.getJdbcConnectionOptions().getUrl());
        Object[][] parameterValues = null;
        try {
            typeInfo = initTableField(jdbcConnectionProvider.getOrEstablishConnection());
            if(parallelism > 1) {
                JdbcParameterValuesProvider jdbcParameterValuesProvider = getParameterValuesProviderAndExtendSql(jdbcConnectionProvider.getOrEstablishConnection());
                if (null != jdbcParameterValuesProvider) {
                    parameterValues = jdbcParameterValuesProvider.getParameterValues();
                }
            }
        } catch (Exception e) {
            throw new PrepareFailException("jdbc", PluginType.SOURCE, e.getMessage());
        }

        inputFormat = new JdbcInputFormat(
            jdbcConnectionProvider,
            jdbcDialect.getRowConverter(),
            typeInfo,
            parameterValues,
            jdbcSourceOptions.getJdbcConnectionOptions().query,
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
        } catch (Exception e) {
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
        return new JdbcSourceSplitEnumerator(enumeratorContext, parallelism);
    }

    @Override
    public SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> restoreEnumerator(SourceSplitEnumerator.Context<JdbcSourceSplit> enumeratorContext, JdbcSourceState checkpointState) throws Exception {
        return new JdbcSourceSplitEnumerator(enumeratorContext, parallelism);
    }

    @Override
    public Serializer<JdbcSourceState> getEnumeratorStateSerializer() {
        return new DefaultSerializer<>();
    }

    private SeaTunnelRowTypeInfo initTableField(Connection conn) {
        JdbcDialectTypeMapper jdbcDialectTypeMapper = jdbcDialect.getJdbcDialectTypeMapper();
        ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
        ArrayList<String> fieldNames = new ArrayList<>();
        try {
            PreparedStatement ps = conn.prepareStatement(jdbcSourceOptions.getJdbcConnectionOptions().getQuery());
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

    private JdbcParameterValuesProvider initPartition(String columnName, String tableName, Connection connection, int parallelism) throws SQLException {
        long max = Long.MAX_VALUE;
        long min = Long.MIN_VALUE;
        if (jdbcSourceOptions.getPartitionLowerBound().isPresent() && jdbcSourceOptions.getPartitionUpperBound().isPresent()) {
            max = jdbcSourceOptions.getPartitionUpperBound().get();
            min = jdbcSourceOptions.getPartitionLowerBound().get();
            return new JdbcNumericBetweenParametersProvider(min, max).ofBatchNum(parallelism);
        }
        try (ResultSet rs = connection.createStatement().executeQuery(String.format("SELECT MAX(%s),MIN(%s) " +
            "FROM %s", columnName, columnName, tableName))) {
            if (rs.next()) {
                max = jdbcSourceOptions.getPartitionUpperBound().isPresent() ? jdbcSourceOptions.getPartitionUpperBound().get() :
                    Long.parseLong(rs.getString(1));
                min = jdbcSourceOptions.getPartitionLowerBound().isPresent() ? jdbcSourceOptions.getPartitionLowerBound().get() :
                    Long.parseLong(rs.getString(2));
            }
        }
        return new JdbcNumericBetweenParametersProvider(min, max).ofBatchNum(parallelism);
    }

    private JdbcParameterValuesProvider getParameterValuesProviderAndExtendSql(Connection connection) throws SQLException {
        String tableName = getTableNameAndFields(query)._1;
        if (null != tableName) {
            if (jdbcSourceOptions.getPartitionColumn().isPresent()) {
                String partitionColumn = jdbcSourceOptions.getPartitionColumn().get();
                Map<String, SeaTunnelDataType<?>> fieldTypes = new HashMap<>();
                for (int i = 0; i < typeInfo.getFieldNames().length; i++) {
                    fieldTypes.put(typeInfo.getFieldNames()[i], typeInfo.getSeaTunnelDataTypes()[0]);
                }
                if (!fieldTypes.containsKey(partitionColumn)) {
                    throw new IllegalArgumentException(String.format("field %s not contain in table %s",
                        partitionColumn, tableName));
                }
                SeaTunnelDataType<?> partitionColumnType = fieldTypes.get(partitionColumn);
                if (!isNumericType(partitionColumnType)) {
                    throw new IllegalArgumentException(String.format("%s is not numeric type", partitionColumn));
                }
                JdbcParameterValuesProvider jdbcParameterValuesProvider =
                    initPartition(partitionColumn, tableName, connection, parallelism);

                query = extendPartitionQuerySql(query, partitionColumn);

                return jdbcParameterValuesProvider;
            } else {
                LOG.info("The partition_column parameter is not configured, and the source parallelism is set to 1");
            }
        } else {
            LOG.info("Failed to parse tableName，and the source parallelism is set to 1");
        }
        parallelism = 1;
        return null;
    }

    private boolean isNumericType(SeaTunnelDataType<?> type) {
        return type.equals(BasicType.INTEGER) || type.equals(BasicType.LONG)
            || type.equals(BasicType.BIG_INTEGER);
    }

}
