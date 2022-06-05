package org.apache.seatunnel.connectors.seatunnel.jdbc.state;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @Author: Liuli
 * @Date: 2022/5/30 23:07
 */
@Data
@AllArgsConstructor
public class JdbcAggregatedCommitInfo implements Serializable
{
    private final List<XidInfo> xidInfoList;
}
