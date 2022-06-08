package org.apache.seatunnel.connectors.seatunnel.jdbc.state;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
@AllArgsConstructor
public class JdbcAggregatedCommitInfo implements Serializable {
    private final List<XidInfo> xidInfoList;
}
