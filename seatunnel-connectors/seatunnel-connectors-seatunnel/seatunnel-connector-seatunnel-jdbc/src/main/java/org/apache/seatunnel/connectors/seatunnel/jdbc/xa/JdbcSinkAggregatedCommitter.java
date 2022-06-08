package org.apache.seatunnel.connectors.seatunnel.jdbc.xa;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class JdbcSinkAggregatedCommitter implements SinkAggregatedCommitter<XidInfo, JdbcAggregatedCommitInfo> {

    private final XaFacade xaFacade;
    private final XaGroupOps xaGroupOps;
    private final JdbcConnectorOptions jdbcConnectorOptions;

    public JdbcSinkAggregatedCommitter(
        JdbcConnectorOptions jdbcConnectorOptions
    ) {
        this.xaFacade = XaFacade.fromJdbcConnectionOptions(
            jdbcConnectorOptions);
        this.xaGroupOps = new XaGroupOpsImpl(xaFacade);
        this.jdbcConnectorOptions = jdbcConnectorOptions;
    }

    @Override
    public List<JdbcAggregatedCommitInfo> commit(List<JdbcAggregatedCommitInfo> aggregatedCommitInfos) {
        List<JdbcAggregatedCommitInfo> collect = aggregatedCommitInfos.stream().map(aggregatedCommitInfo -> {
            XaGroupOps.GroupXaOperationResult<XidInfo> result = xaGroupOps.commit(aggregatedCommitInfo.getXidInfoList(), false, jdbcConnectorOptions.getMaxCommitAttempts());
            return new JdbcAggregatedCommitInfo(result.getForRetry());
        }).collect(Collectors.toList());
        return collect;
    }

    @Override
    public JdbcAggregatedCommitInfo combine(List<XidInfo> commitInfos) {
        return new JdbcAggregatedCommitInfo(commitInfos);
    }

    @Override
    public void abort(List<JdbcAggregatedCommitInfo> aggregatedCommitInfo) {
        for (JdbcAggregatedCommitInfo commitInfos : aggregatedCommitInfo) {
            xaGroupOps.rollback(commitInfos.getXidInfoList());
        }
    }

    @Override
    public void close()
        throws IOException {
        try {
            xaFacade.close();
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }
}
