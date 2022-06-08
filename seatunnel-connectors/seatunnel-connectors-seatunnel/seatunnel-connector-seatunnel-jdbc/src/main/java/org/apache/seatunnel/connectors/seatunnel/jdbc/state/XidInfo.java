package org.apache.seatunnel.connectors.seatunnel.jdbc.state;

import lombok.AllArgsConstructor;
import lombok.Data;

import javax.transaction.xa.Xid;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class XidInfo implements Serializable {

    final Xid xid;
    final int attempts;

    public XidInfo withAttemptsIncremented() {
        return new XidInfo(xid, attempts + 1);
    }
}
