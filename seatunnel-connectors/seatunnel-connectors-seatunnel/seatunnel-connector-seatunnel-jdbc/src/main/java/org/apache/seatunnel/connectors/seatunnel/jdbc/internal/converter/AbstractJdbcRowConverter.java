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

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.api.table.type.TimestampType;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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
    @SuppressWarnings("checkstyle:Indentation")
    public SeaTunnelRow toInternal(ResultSet rs, ResultSetMetaData metaData, SeaTunnelRowTypeInfo typeInfo) throws SQLException {

        List<Object> fields = new ArrayList<>();
        Map<String, Object> fieldMap = new HashMap<>();
        SeaTunnelDataType<?>[] seaTunnelDataTypes = typeInfo.getSeaTunnelDataTypes();

        for (int i = 1; i <= seaTunnelDataTypes.length; i++) {
            Object seatunnelField;
            String name = metaData.getColumnName(i);
            SeaTunnelDataType<?> seaTunnelDataType = seaTunnelDataTypes[i - 1];
            if (seaTunnelDataType instanceof TimestampType) {
                TimestampType timestampType = (TimestampType) seaTunnelDataType;
                seatunnelField = rs.getTimestamp(i);
            } else if (BasicType.BOOLEAN.equals(seaTunnelDataType)) {
                seatunnelField = rs.getBoolean(i);
            } else if (BasicType.BYTE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getByte(i);
            } else if (BasicType.SHORT.equals(seaTunnelDataType)) {
                seatunnelField = rs.getShort(i);
            } else if (BasicType.INTEGER.equals(seaTunnelDataType)) {
                seatunnelField = rs.getInt(i);
            } else if (BasicType.BIG_INTEGER.equals(seaTunnelDataType)) {
                seatunnelField = (BigInteger) rs.getObject(i);
            } else if (BasicType.LONG.equals(seaTunnelDataType)) {
                seatunnelField = rs.getLong(i);
            } else if (BasicType.BIG_DECIMAL.equals(seaTunnelDataType)) {
                seatunnelField = rs.getBigDecimal(i);
            } else if (BasicType.FLOAT.equals(seaTunnelDataType)) {
                seatunnelField = rs.getFloat(i);
            } else if (BasicType.DOUBLE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getDouble(i);
            } else if (BasicType.STRING.equals(seaTunnelDataType)) {
                seatunnelField = rs.getString(i);
            } else if (BasicType.DATE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getObject(i);
            } else if (new ArrayType<>(BasicType.BYTE).equals(seaTunnelDataType)) {
                seatunnelField = rs.getByte(i);
            } else {
                throw new IllegalStateException("Unexpected value: " + seaTunnelDataType);
            }

            fields.add(seatunnelField);
            fieldMap.put(name, seatunnelField);
        }

        return new SeaTunnelRow(fields.toArray(), fieldMap);
    }
}
