package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.TimestampType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * @Author: Liuli
 * @Date: 2022/6/14 15:12
 */
public class MySqlTypeMapper implements JdbcDialectTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDialect.class);

    // ============================data types=====================

    private static final String MYSQL_UNKNOWN = "UNKNOWN";
    private static final String MYSQL_BIT = "BIT";

    // -------------------------number----------------------------
    private static final String MYSQL_TINYINT = "TINYINT";
    private static final String MYSQL_TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    private static final String MYSQL_SMALLINT = "SMALLINT";
    private static final String MYSQL_SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    private static final String MYSQL_MEDIUMINT = "MEDIUMINT";
    private static final String MYSQL_MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    private static final String MYSQL_INT = "INT";
    private static final String MYSQL_INT_UNSIGNED = "INT UNSIGNED";
    private static final String MYSQL_INTEGER = "INTEGER";
    private static final String MYSQL_INTEGER_UNSIGNED = "INTEGER UNSIGNED";
    private static final String MYSQL_BIGINT = "BIGINT";
    private static final String MYSQL_BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    private static final String MYSQL_DECIMAL = "DECIMAL";
    private static final String MYSQL_DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    private static final String MYSQL_FLOAT = "FLOAT";
    private static final String MYSQL_FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    private static final String MYSQL_DOUBLE = "DOUBLE";
    private static final String MYSQL_DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";

    // -------------------------string----------------------------
    private static final String MYSQL_CHAR = "CHAR";
    private static final String MYSQL_VARCHAR = "VARCHAR";
    private static final String MYSQL_TINYTEXT = "TINYTEXT";
    private static final String MYSQL_MEDIUMTEXT = "MEDIUMTEXT";
    private static final String MYSQL_TEXT = "TEXT";
    private static final String MYSQL_LONGTEXT = "LONGTEXT";
    private static final String MYSQL_JSON = "JSON";

    // ------------------------------time-------------------------
    private static final String MYSQL_DATE = "DATE";
    private static final String MYSQL_DATETIME = "DATETIME";
    private static final String MYSQL_TIME = "TIME";
    private static final String MYSQL_TIMESTAMP = "TIMESTAMP";
    private static final String MYSQL_YEAR = "YEAR";

    // ------------------------------blob-------------------------
    private static final String MYSQL_TINYBLOB = "TINYBLOB";
    private static final String MYSQL_MEDIUMBLOB = "MEDIUMBLOB";
    private static final String MYSQL_BLOB = "BLOB";
    private static final String MYSQL_LONGBLOB = "LONGBLOB";
    private static final String MYSQL_BINARY = "BINARY";
    private static final String MYSQL_VARBINARY = "VARBINARY";
    private static final String MYSQL_GEOMETRY = "GEOMETRY";

    // TODO unSupport
    String databaseVersion;
    String driverVersion;

    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String mysqlType = metadata.getColumnTypeName(colIndex).toUpperCase();
        String columnName = metadata.getColumnName(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);

        System.out.println("------>"+mysqlType);
        switch (mysqlType) {
            case MYSQL_BIT:
                return BasicType.BOOLEAN;
            case MYSQL_TINYBLOB:
            case MYSQL_MEDIUMBLOB:
            case MYSQL_BLOB:
            case MYSQL_LONGBLOB:
            case MYSQL_VARBINARY:
            case MYSQL_BINARY:
                // BINARY is not supported in MySqlDialect now.
                // VARBINARY(n) is not supported in MySqlDialect when 'n' is not equals to
                // Integer.MAX_VALUE. Please see
                // org.apache.flink.connector.jdbc.dialect.mysql.MySqlDialect#supportedTypes and
                // org.apache.flink.connector.jdbc.dialect.AbstractDialect#validate for more
                // details.
                return BasicType.BYTE;
            case MYSQL_TINYINT:
            case MYSQL_TINYINT_UNSIGNED:
            case MYSQL_SMALLINT:
                return BasicType.SHORT;
            case MYSQL_SMALLINT_UNSIGNED:
            case MYSQL_MEDIUMINT:
            case MYSQL_MEDIUMINT_UNSIGNED:
            case MYSQL_INT:
            case MYSQL_INTEGER:
                return BasicType.INTEGER;
            case MYSQL_INT_UNSIGNED:
            case MYSQL_INTEGER_UNSIGNED:
            case MYSQL_BIGINT:
                return BasicType.BIG_INTEGER;
            //TODO DECIMAL not set precision , scale
            case MYSQL_BIGINT_UNSIGNED:
            case MYSQL_DECIMAL:
            case MYSQL_DECIMAL_UNSIGNED:
                return BasicType.BIG_DECIMAL;
            case MYSQL_FLOAT:
                return BasicType.FLOAT;
            case MYSQL_FLOAT_UNSIGNED:
                LOG.warn("{} will probably cause value overflow.", MYSQL_FLOAT_UNSIGNED);
                return BasicType.FLOAT;
            case MYSQL_DOUBLE:
                return BasicType.DOUBLE;
            case MYSQL_DOUBLE_UNSIGNED:
                LOG.warn("{} will probably cause value overflow.", MYSQL_DOUBLE_UNSIGNED);
                return BasicType.DOUBLE;
            //TODO CHAR not set precision
            case MYSQL_CHAR:
            case MYSQL_TINYTEXT:
            case MYSQL_MEDIUMTEXT:
            case MYSQL_TEXT:
                return BasicType.CHARACTER;
            case MYSQL_VARCHAR:
                return BasicType.STRING;
            case MYSQL_JSON:
                return BasicType.STRING;
            case MYSQL_LONGTEXT:
                LOG.warn(
                    "Type '{}' has a maximum precision of 536870911 in MySQL. "
                        + "Due to limitations in the seatunnel type system, "
                        + "the precision will be set to 2147483647.",
                    MYSQL_LONGTEXT);
                return BasicType.STRING;
            case MYSQL_DATE:
            case MYSQL_TIME:
            case MYSQL_DATETIME:
                return BasicType.DATE;
            case MYSQL_TIMESTAMP:
                return new TimestampType(precision);
            case MYSQL_YEAR:
            case MYSQL_GEOMETRY:
            case MYSQL_UNKNOWN:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new UnsupportedOperationException(
                    String.format(
                        "Doesn't support MySQL type '%s' on column '%s' in MySQL version %s, driver version %s yet.",
                        mysqlType, jdbcColumnName, databaseVersion, driverVersion));
        }
    }
}
