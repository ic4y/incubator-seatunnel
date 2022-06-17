package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSourceState;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @Author: Liuli
 * @Date: 2022/6/15 16:03
 */
public class JdbcSourceSplitEnumerator implements SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> {
    BlockingQueue<JdbcSourceSplit> splits = new LinkedBlockingQueue<>();
    BlockingQueue<Integer> registedReader = new LinkedBlockingQueue<>();
    SourceSplitEnumerator.Context<JdbcSourceSplit> enumeratorContext;

    public JdbcSourceSplitEnumerator(Context<JdbcSourceSplit> enumeratorContext) {
        this.enumeratorContext = enumeratorContext;
    }

    @Override
    public void open() {
        splits.add(new JdbcSourceSplit(Optional.empty()));
    }

    @Override
    public void run() throws Exception {
        Integer taskId = registedReader.take();
        JdbcSourceSplit split = splits.take();
        enumeratorContext.assignSplit(taskId, split);
        enumeratorContext.signalNoMoreSplits(taskId);
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void addSplitsBack(List<JdbcSourceSplit> splits, int subtaskId) {
        this.splits.addAll(splits);
    }

    @Override
    public int currentUnassignedSplitSize() {
        return splits.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {

    }

    @Override
    public void registerReader(int subtaskId) {
        registedReader.add(subtaskId);
    }

    @Override
    public JdbcSourceState snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }
}
