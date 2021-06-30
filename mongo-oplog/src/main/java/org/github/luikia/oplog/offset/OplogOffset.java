package org.github.luikia.oplog.offset;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonTimestamp;
import org.github.luikia.offset.Offset;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class OplogOffset extends Offset {

    private static final long serialVersionUID = 1L;

    private long ts;

    public OplogOffset(BsonTimestamp ts) {
        this(ts.getValue());
    }

    @Override
    public String toJsonString() {
        return g.toJson(this);
    }

    public static OplogOffset fromJson(String json) {
        return StringUtils.isNoneEmpty(json) ? g.fromJson(json, OplogOffset.class) : null;
    }
}
