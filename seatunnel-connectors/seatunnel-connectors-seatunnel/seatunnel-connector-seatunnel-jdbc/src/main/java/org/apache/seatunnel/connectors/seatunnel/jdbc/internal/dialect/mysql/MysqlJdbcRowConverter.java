package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class MysqlJdbcRowConverter extends AbstractJdbcRowConverter {
    @Override
    public String converterName() {
        return "MySql";
    }

    @Override
    public SeaTunnelRow toInternal(ResultSet rs, ResultSetMetaData metaData, SeaTunnelRowTypeInfo typeInfo) throws SQLException {
        return super.toInternal(rs, metaData, typeInfo);
    }
}
