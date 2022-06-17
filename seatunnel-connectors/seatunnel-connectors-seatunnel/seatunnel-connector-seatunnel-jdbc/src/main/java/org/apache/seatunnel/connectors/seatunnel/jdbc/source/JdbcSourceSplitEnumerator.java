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
    int parallelism = 0;

    public JdbcSourceSplitEnumerator(Context<JdbcSourceSplit> enumeratorContext, int parallelism) {
        this.enumeratorContext = enumeratorContext;
        this.parallelism = parallelism;
    }

    @Override
    public void open() {
        for (int i = 0; i < parallelism; i++) {
            splits.add(new JdbcSourceSplit(i));
        }
    }

    @Override
    public void run() throws Exception {
        while (!splits.isEmpty()) {
            for (Integer readerId : registedReader) {
                JdbcSourceSplit split = splits.poll();
                if (split == null) {
                    break;
                } else {
                    enumeratorContext.assignSplit(readerId, split);
                }
            }
        }
        for (Integer rederId : registedReader) {
            enumeratorContext.signalNoMoreSplits(rederId);
        }
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
