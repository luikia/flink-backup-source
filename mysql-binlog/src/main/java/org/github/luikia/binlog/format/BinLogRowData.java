package org.github.luikia.binlog.format;

import com.google.common.collect.ImmutableMap;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.github.luikia.type.LogType;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
public class BinLogRowData implements Serializable, Iterable<BinLogRowData.RowData> {

    public static final TypeInformation<BinLogRowData> TYPE = Types.POJO(BinLogRowData.class, ImmutableMap.of(
            "database", Types.STRING,
            "table", Types.STRING,
            "type", Types.ENUM(LogType.class),
            "timestamp", Types.LONG,
            "rows", RowData.TYPE
    ));
    private static final long serialVersionUID = 1L;

    private String database;

    private String table;

    private LogType type;

    private long timestamp;

    private List<RowData> rows;

    @Override
    public Iterator<RowData> iterator() {
        return rows.iterator();
    }

    @Data
    @NoArgsConstructor
    public static class RowData implements Serializable {

        private static final long serialVersionUID = 1L;

        public static final TypeInformation<RowData> TYPE = Types.GENERIC(RowData.class);

        private Map<String, Serializable> data;

        private Map<String, Serializable> old;
    }
    

}
