package org.github.luikia.oplog;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.github.luikia.BackupSourceFunction;
import org.github.luikia.oplog.client.ChangeStreamClient;
import org.github.luikia.oplog.format.ChangeStreamData;
import org.github.luikia.oplog.offset.ChangeStreamOffset;

import java.util.Objects;

public class ChangeStreamSourceFunction extends BackupSourceFunction<ChangeStreamData, ChangeStreamOffset> {

    private String url;

    private ChangeStreamOffset offset;

    private transient ChangeStreamClient client;

    public ChangeStreamSourceFunction(String url, String resumeToken) {
        this(url);
        this.offset = new ChangeStreamOffset(resumeToken);
    }

    public ChangeStreamSourceFunction(String url) {
        this.url = url;
    }


    @Override
    public ChangeStreamOffset getOffset() {
        return offset;
    }

    @Override
    public void setOffset(ChangeStreamOffset offset) {
        this.offset = offset;
    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    protected ChangeStreamOffset formOffsetJson(String json) {
        return ChangeStreamOffset.fromJson(json);
    }

    @Override
    public TypeInformation<ChangeStreamData> getProducedType() {
        return ChangeStreamData.TYPES;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        ConnectionString conn = new ConnectionString(this.url);
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(conn).build();
        this.client = new ChangeStreamClient(settings);
        if (Objects.nonNull(this.offset) && StringUtils.isNotEmpty(this.offset.getResumeToken()))
            this.client.setResumeToken(this.offset.getResumeToken());
        this.startZKClient();
    }

    @Override
    public void run(SourceContext<ChangeStreamData> ctx) throws Exception {
        final Object lock = ctx.getCheckpointLock();
        if (this.getLock()) {
            initOffset();
            client.start(e -> {
                synchronized (lock) {
                    if (Objects.isNull(this.offset))
                        this.offset = new ChangeStreamOffset(e.getResumeToken());
                    else
                        this.offset.setResumeToken(e.getResumeToken());
                }
                ctx.collect(e);
            });
        }
    }

    @Override
    public void cancel() {
        this.client.close();
    }
}
