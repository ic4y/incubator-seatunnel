package org.apache.seatunnel.connectors.seatunnel.console.sink;

import org.apache.seatunnel.api.sink.SinkCommitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Liuli
 * @Date: 2022/6/5 00:53
 */
public class ConsoleCommitter implements SinkCommitter<ConsoleCommitInfo>
{
    public ConsoleCommitter()
    {
        System.out.println("build ConsoleCommitter");
    }

    @Override
    public List<ConsoleCommitInfo> commit(List<ConsoleCommitInfo> committables)
            throws IOException
    {
        System.out.println("commited --->" + committables.get(0).getCommitInfo());
        return new ArrayList<>();
    }

    @Override
    public void abort(List<ConsoleCommitInfo> commitInfos)
            throws IOException
    {
        System.out.println("------abort------");
    }
}
