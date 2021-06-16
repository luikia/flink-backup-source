package org.github.luikia.binlog.offset;

import com.github.shyiko.mysql.binlog.event.RotateEventData;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.github.luikia.offset.Offset;

@Data
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class BinLogOffset extends Offset {

    private static final long serialVersionUID = 1L;

    private String binlogFilename;

    private long binlogPosition;

    private BinLogOffset(RotateEventData eventData) {
        this.binlogFilename = eventData.getBinlogFilename();
        this.binlogPosition = eventData.getBinlogPosition();
    }

    public static BinLogOffset fromJson(String json) {
        return StringUtils.isNoneEmpty(json) ? g.fromJson(json, BinLogOffset.class) : null;
    }

    @Override
    public String toJsonString() {
        return g.toJson(this);
    }

    public static BinLogOffset of(String binlogFilename, long binlogPosition) {
        return new BinLogOffset(binlogFilename, binlogPosition);
    }

    public static BinLogOffset of(RotateEventData eventData) {
        return new BinLogOffset(eventData);
    }

}
