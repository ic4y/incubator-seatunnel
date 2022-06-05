package org.apache.seatunnel.connectors.seatunnel.jdbc.xa;

import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcExactlyOnceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.SerializableSupplier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.xa.XaFacade;
import org.apache.seatunnel.connectors.seatunnel.jdbc.xa.XaGroupOps;
import org.apache.seatunnel.connectors.seatunnel.jdbc.xa.XaGroupOpsImpl;

import javax.sql.XADataSource;

import java.io.IOException;
import java.util.List;

/**
 * @Author: Liuli
 * @Date: 2022/5/30 23:14
 */
public class JdbcSinkCommitter
        implements SinkCommitter<XidInfo>
{
    private final XaFacade xaFacade;
    private final XaGroupOps xaGroupOps;

    public JdbcSinkCommitter(
            JdbcExactlyOnceOptions exactlyOnceOptions,
            SerializableSupplier<XADataSource> dataSourceSupplier
    )
    {
        this.xaFacade = XaFacade.fromXaDataSourceSupplier(
                dataSourceSupplier,
                exactlyOnceOptions.getTimeoutSec());
        this.xaGroupOps = new XaGroupOpsImpl(xaFacade);
        System.out.println("------JdbcSinkCommitter---"+xaFacade.toString());
    }

    @Override
    public List<XidInfo> commit(List<XidInfo> committables)
            throws IOException
    {
        System.out.println("-----commit--->" + committables.get(0).getXid().toString());
        XaGroupOps.GroupXaOperationResult<XidInfo> result = xaGroupOps.commit(committables, false, 10);
        List<XidInfo> forRetry = result.getForRetry();
        return forRetry;
    }

    @Override
    public void abort(List<XidInfo> commitInfos)
            throws IOException
    {
        System.out.println("--------_>abort");
        xaGroupOps.rollback(commitInfos);
    }
}
