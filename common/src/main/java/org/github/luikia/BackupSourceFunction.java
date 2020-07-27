package org.github.luikia;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.github.luikia.offset.Offset;
import org.github.luikia.zk.ZkClient;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class BackupSourceFunction<T,OFFSET extends Offset> extends RichParallelSourceFunction<T> implements CheckpointedFunction, CheckpointListener {

    protected ZkClient zkClient;

    private transient ListState<String> offsetState;

    protected boolean getLock() {
        int partitionIndex = getRuntimeContext().getIndexOfThisSubtask();
        if (Objects.isNull(this.zkClient)) {
            return true;
        } else {
            InterProcessMutex lock = this.zkClient.getLock();
            try {
                while (true) {
                    if (lock.acquire(10L, TimeUnit.SECONDS)) {
                        log.info("partition {} has require zk lock", partitionIndex);
                        return true;
                    } else {
                        log.debug("patition {} not require zk lock", partitionIndex);
                    }
                }
            } catch (Exception e) {
                log.error("get zookeeper lock fail", e);
                return false;
            }
        }
    }

    public abstract OFFSET getOffset();

    public abstract void setOffset(OFFSET offset);

    public abstract boolean isRunning();

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (Objects.nonNull(this.zkClient) && this.isRunning() && Objects.nonNull(this.getOffset()))
            this.zkClient.saveOffset(this.getOffset());
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        this.offsetState.clear();
        if (Objects.nonNull(this.getOffset()) && isRunning()) {
            this.offsetState.add(this.getOffset().toJsonString());
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        OperatorStateStore stateStore = context.getOperatorStateStore();
        offsetState = stateStore.getUnionListState(new ListStateDescriptor("offset", Types.STRING));
        if (context.isRestored() && offsetState.get().iterator().hasNext() && Objects.isNull(this.getOffset())) {
            this.setOffset(formJson(offsetState.get().iterator().next()));
        }
        offsetState.clear();
    }

    protected abstract OFFSET formJson(String json);

    protected void startZKClient(){
        if (Objects.nonNull(zkClient)) {
            try {
                zkClient.start();
            } catch (Exception e) {
                log.error("zk client start error", e);
                zkClient = null;
            }
        }
    }

    public ZkClient getZkClient() {
        return zkClient;
    }

    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
    }
}
