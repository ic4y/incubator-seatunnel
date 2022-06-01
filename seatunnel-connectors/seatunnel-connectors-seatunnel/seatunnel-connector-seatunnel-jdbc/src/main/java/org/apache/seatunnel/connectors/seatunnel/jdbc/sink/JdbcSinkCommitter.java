package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcCommitInfo;

import java.io.IOException;
import java.util.List;

/**
 * @Author: Liuli
 * @Date: 2022/5/30 23:14
 */
public class JdbcSinkCommitter implements SinkCommitter<JdbcCommitInfo>
{
    @Override
    public List<JdbcCommitInfo> commit(List<JdbcCommitInfo> committables)
            throws IOException
    {
        return null;
    }

    @Override
    public void abort(List<JdbcCommitInfo> commitInfos)
            throws IOException
    {

    }
}
