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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for all converters that convert between JDBC object and Flink internal object.
 */
public abstract class AbstractJdbcRowConverter implements JdbcRowConverter {

    public abstract String converterName();

    public AbstractJdbcRowConverter() {
    }

    @Override
    public SeaTunnelRow toInternal(ResultSet rs, ResultSetMetaData metaData) throws SQLException {

        List<Object> fields = new ArrayList<>();
        Map<String, Object> fieldMap = new HashMap<>();

        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            Object seatunnelField;
            String name = metaData.getColumnName(i);
            int columnType = metaData.getColumnType(i);
            switch (columnType) {
                case Types.CHAR:
                case Types.NCHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    seatunnelField = rs.getString(i);
                    break;

                case Types.CLOB:
                case Types.NCLOB:
                    seatunnelField = rs.getString(i);
                    break;

                case Types.SMALLINT:
                case Types.TINYINT:
                case Types.INTEGER:
                    seatunnelField = rs.getInt(i);
                    break;
                case Types.BIGINT:
                    seatunnelField = rs.getLong(i);
                    break;

                case Types.NUMERIC:
                case Types.DECIMAL:
                    seatunnelField = rs.getBigDecimal(i);
                    break;

                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE:
                    seatunnelField = rs.getDouble(i);
                    break;

                case Types.TIME:
                    seatunnelField = rs.getTime(i);
                    break;

                case Types.DATE:
                    seatunnelField = rs.getDate(i);
                    break;

                case Types.TIMESTAMP:
                    seatunnelField = rs.getTimestamp(i);
                    break;

                case Types.BINARY:
                case Types.VARBINARY:
                case Types.BLOB:
                case Types.LONGVARBINARY:
                    seatunnelField = rs.getBytes(i);
                    break;

                case Types.BOOLEAN:
                case Types.BIT:
                    seatunnelField = rs.getBoolean(i);
                    break;

                case Types.NULL:
                    seatunnelField = rs.getObject(i);
                    break;

                default:
                    throw new UnsupportedOperationException("Unsupported type:" + columnType);
            }
            fields.add(seatunnelField);
            fieldMap.put(name, seatunnelField);
        }

        return new SeaTunnelRow(fields.toArray(), fieldMap);
    }
}
