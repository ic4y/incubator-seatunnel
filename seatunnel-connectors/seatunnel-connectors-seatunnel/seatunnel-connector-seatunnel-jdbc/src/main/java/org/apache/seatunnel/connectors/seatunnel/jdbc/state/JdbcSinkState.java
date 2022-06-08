package org.apache.seatunnel.connectors.seatunnel.jdbc.state;

import lombok.AllArgsConstructor;
import lombok.Data;

import javax.transaction.xa.Xid;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class JdbcSinkState implements Serializable {
    private final Xid xid;
}
