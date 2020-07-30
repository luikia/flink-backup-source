package org.github.luikia.oplog;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.github.luikia.BackupSourceFunction;
import org.github.luikia.oplog.client.OplogClient;
import org.github.luikia.oplog.format.OplogData;
import org.github.luikia.oplog.offset.OplogOffset;

import java.util.Objects;

@Slf4j
public class OplogSourceFunction extends BackupSourceFunction<OplogData, OplogOffset> {

    private static final long serialVersionUID = 1L;

    private String url;

    private OplogOffset offset;

    private transient OplogClient client;

    public OplogSourceFunction(String url) {
        this.url = url;
    }

    @Override
    public void open(Configuration parameters) {
        ConnectionString conn = new ConnectionString(this.url);
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(conn).build();
        this.client = new OplogClient(settings);
        if (Objects.nonNull(this.offset) && this.offset.getTs() != 0L) {
            this.client.setOffset(this.offset.getTs());
        }
        this.startZKClient();
    }

    @Override
    public OplogOffset getOffset() {
        return offset;
    }

    @Override
    public void setOffset(OplogOffset offset) {
        this.offset = offset;
    }

    @Override
    public boolean isRunning() {
        return this.client.isRunning();
    }

    @Override
    protected OplogOffset formJson(String json) {
        return OplogOffset.fromJson(json);
    }

    @Override
    public void run(SourceFunction.SourceContext<OplogData> ctx) throws Exception {
        final Object lock = ctx.getCheckpointLock();
        this.client.setCallback(r -> {
            synchronized (lock) {
                if (Objects.isNull(this.offset))
                    this.offset = new OplogOffset(r.getOffset());
                else
                    this.offset.setTs(r.getOffset());
                if (StringUtils.isNoneEmpty(r.getData()))
                    ctx.collect(r);
            }
        });
        if (this.getLock()) {
            if (Objects.isNull(this.offset) && Objects.nonNull(zkClient))
                this.offset = this.formJson(this.zkClient.getOffsetJson());
            if (Objects.nonNull(this.offset) && this.offset.getTs() != 0L)
                client.setOffset(this.offset.getTs());
            client.start();
        }
    }

    @Override
    public void cancel() {
        try {
            this.client.close();
        } catch (Exception e) {
            log.error("stop client error", e);
        }
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

}
