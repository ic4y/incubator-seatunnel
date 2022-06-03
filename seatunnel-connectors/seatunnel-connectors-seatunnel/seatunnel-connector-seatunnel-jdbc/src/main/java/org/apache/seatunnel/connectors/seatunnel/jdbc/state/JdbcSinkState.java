package org.apache.seatunnel.connectors.seatunnel.jdbc.state;

import lombok.AllArgsConstructor;
import lombok.Data;

import javax.transaction.xa.Xid;

import java.io.Serializable;

/**
 * @Author: Liuli
 * @Date: 2022/5/30 23:04
 */
@Data
@AllArgsConstructor
public class JdbcSinkState implements Serializable
{
    private final Xid xid;
}
