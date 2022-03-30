package org.github.luikia.replication;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.github.luikia.BackupSourceFunction;
import org.github.luikia.replication.client.ReplicationClient;
import org.github.luikia.replication.format.ReplicationData;
import org.github.luikia.replication.offset.LsnOffset;

import java.util.Objects;

@Slf4j
public abstract class ReplicationSourceFunction<T> extends BackupSourceFunction<T, LsnOffset> {
    private static final long serialVersionUID = 1L;

    private String url;

    private String username;

    private String password;

    private String soltName;

    private String minVersion = "9.4";

    private LsnOffset startLsn;

    private transient ReplicationClient client;

    public ReplicationSourceFunction(String url, String username, String password, String soltName) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.soltName = soltName;
    }

    @Override
    public void open(Configuration parameters) {
        this.client = new ReplicationClient(this.url, this.username, this.password, this.soltName, this.minVersion);
        if (Objects.nonNull(this.startLsn) && StringUtils.isNoneEmpty(this.startLsn.getLsn())) {
            client.setStartLsn(this.startLsn.getLogSequenceNumber());
        }
        this.startZKClient();
    }

    @Override
    public void run(SourceFunction.SourceContext<T> ctx) throws Exception {
        final Object lock = ctx.getCheckpointLock();
        client.setCallBack(r -> {
            synchronized (lock) {
                if (Objects.isNull(this.startLsn))
                    this.startLsn = LsnOffset.of(r.getLsn());
                else
                    this.startLsn.setLsn(r.getLsn());
                if (Objects.nonNull(r.getChanges()))
                    ctx.collect(format(r));
            }
        });
        if (this.getLock()) {
            initOffset();
            if (Objects.nonNull(this.startLsn) && StringUtils.isNoneEmpty(this.startLsn.getLsn())) {
                client.setStartLsn(this.startLsn.getLogSequenceNumber());
            }
            client.start();
        }

    }

    public abstract T format(ReplicationData data);

    @Override
    public void cancel() {
        try {
            this.client.close();
        } catch (Exception e) {
            log.error("stop client error", e);
        }
    }

    @Override
    public LsnOffset formOffsetJson(String json) {
        return LsnOffset.fromJson(json);
    }

    public String getMinVersion() {
        return minVersion;
    }

    public void setMinVersion(String minVersion) {
        this.minVersion = minVersion;
    }

    @Override
    public LsnOffset getOffset() {
        return this.startLsn;
    }

    @Override
    public void setOffset(LsnOffset offset) {
        this.startLsn = offset;
    }

    @Override
    public boolean isRunning() {
        return this.client.isRunning();
    }

}
