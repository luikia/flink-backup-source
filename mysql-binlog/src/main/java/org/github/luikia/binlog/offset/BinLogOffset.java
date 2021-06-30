package org.github.luikia.binlog.offset;

import com.github.shyiko.mysql.binlog.GtidSet;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import lombok.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.github.luikia.offset.Offset;

import java.util.Collections;

@Data
@NoArgsConstructor
public class BinLogOffset extends Offset {

    private static final long serialVersionUID = 1L;

    private String binlogFilename;

    private long binlogPosition;

    private String gtidSet;

    private BinLogOffset(RotateEventData eventData) {
        this.binlogFilename = eventData.getBinlogFilename();
        this.binlogPosition = eventData.getBinlogPosition();
    }

    private BinLogOffset(String binlogFilename, long binlogPosition) {
        this.binlogFilename = binlogFilename;
        this.binlogPosition = binlogPosition;
    }

    private BinLogOffset(String gtidSet) {
        this.gtidSet = gtidSet;
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

    public static BinLogOffset of(String gtidSet) {
        return new BinLogOffset(gtidSet);
    }

    public static BinLogOffset of(RotateEventData eventData) {
        return new BinLogOffset(eventData);
    }

    public static BinLogOffset of(GtidEventData eventData) {
        String gtid = eventData.getGtid();
        String[] split = StringUtils.split(gtid, ":");
        if (split.length != 2 && !StringUtils.isNumeric(split[1]))
            return new BinLogOffset();
        else {
            split[1] = new GtidSet.Interval(1L,NumberUtils.toLong(split[1])).toString();
            return new BinLogOffset(StringUtils.join(split, ":"));
        }
    }

    public BinLogOffset mergeGtid(GtidEventData eventData) {
        GtidSet gtidSet = new GtidSet(this.getGtidSet());
        String gtid = eventData.getGtid();
        String[] split = StringUtils.split(gtid, ":");
        if (split.length < 2 || !StringUtils.isNumeric(split[1])) {
            return this;
        }

        GtidSet.Interval interval = new GtidSet.Interval(1L, NumberUtils.toLong(split[1]));
        GtidSet.UUIDSet uuidSet = new GtidSet.UUIDSet(split[0], Collections.singletonList(interval));
        gtidSet.putUUIDSet(uuidSet);
        this.gtidSet = gtidSet.toString();
        return this;
    }

}
