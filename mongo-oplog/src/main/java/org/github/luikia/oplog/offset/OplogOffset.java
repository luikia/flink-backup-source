package org.github.luikia.oplog.offset;

import org.apache.commons.lang3.StringUtils;
import org.bson.BsonTimestamp;
import org.github.luikia.offset.Offset;

public class OplogOffset extends Offset {

    private static final long serialVersionUID = 1L;

    private long ts;

    public OplogOffset(BsonTimestamp ts) {
        this(ts.getValue());
    }

    public OplogOffset(long ts) {
        this.ts = ts;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    @Override
    public String toJsonString() {
        return g.toJson(this);
    }

    public static OplogOffset fromJson(String json) {
        return StringUtils.isNoneEmpty(json) ? g.fromJson(json, OplogOffset.class) : null;
    }
}
