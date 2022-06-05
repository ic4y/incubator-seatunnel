package org.apache.seatunnel.connectors.seatunnel.jdbc.xa;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcExactlyOnceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.SerializableSupplier;

import javax.sql.XADataSource;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Author: Liuli
 * @Date: 2022/5/30 23:21
 */
public class JdbcSinkAggregatedCommitter implements SinkAggregatedCommitter<XidInfo, JdbcAggregatedCommitInfo>
{

    private final XaFacade xaFacade;
    private final XaGroupOps xaGroupOps;

    public JdbcSinkAggregatedCommitter(
            JdbcExactlyOnceOptions exactlyOnceOptions,
            SerializableSupplier<XADataSource> dataSourceSupplier
    )
    {
        this.xaFacade = XaFacade.fromXaDataSourceSupplier(
                dataSourceSupplier,
                exactlyOnceOptions.getTimeoutSec());
        this.xaGroupOps = new XaGroupOpsImpl(xaFacade);
    }

    @Override
    public List<JdbcAggregatedCommitInfo> commit(List<JdbcAggregatedCommitInfo> aggregatedCommitInfos)
            throws IOException
    {
        List<JdbcAggregatedCommitInfo> collect = aggregatedCommitInfos.stream().map(aggregatedCommitInfo -> {
            XaGroupOps.GroupXaOperationResult<XidInfo> result = xaGroupOps.commit(aggregatedCommitInfo.getXidInfoList(), false, 10);
            return new JdbcAggregatedCommitInfo(result.getForRetry());
        }).collect(Collectors.toList());
       return collect;
    }

    @Override
    public JdbcAggregatedCommitInfo combine(List<XidInfo> commitInfos)
    {
        return new JdbcAggregatedCommitInfo(commitInfos);
    }

    @Override
    public void abort(List<JdbcAggregatedCommitInfo> aggregatedCommitInfo)
            throws Exception
    {
        for(JdbcAggregatedCommitInfo commitInfos : aggregatedCommitInfo){
            xaGroupOps.rollback(commitInfos.getXidInfoList());
        }
    }

    @Override
    public void close()
            throws IOException
    {
//        try {
//            xaFacade.close();
//        }
//        catch (Exception e) {
//            throw new IOException(e);
//        }
    }
}
