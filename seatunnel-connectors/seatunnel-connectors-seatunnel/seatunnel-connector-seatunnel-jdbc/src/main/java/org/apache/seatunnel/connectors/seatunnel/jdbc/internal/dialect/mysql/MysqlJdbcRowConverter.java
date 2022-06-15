package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;

/**
 * @Author: Liuli
 * @Date: 2022/6/15 17:47
 */
public class MysqlJdbcRowConverter extends AbstractJdbcRowConverter {
    @Override
    public String converterName() {
        return "mysql";
    }
}
