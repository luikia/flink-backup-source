package org.github.luikia.binlog.table;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import com.github.shyiko.mysql.binlog.network.protocol.ResultSetRowPacket;
import com.github.shyiko.mysql.binlog.network.protocol.command.QueryCommand;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.github.luikia.binlog.query.QueryClient;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Data
@ToString
public class TableDesc implements Serializable {

    private static final String QUERY_COLUME_NAME_SQL = "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS t WHERE t.TABLE_SCHEMA='%s' AND t.TABLE_NAME='%s' ORDER BY t.ORDINAL_POSITION";

    private long tableId;

    private String database;

    private String table;

    private List<ColumnDesc> columns;

    public TableDesc(TableMapEventData eventData, QueryClient client) throws Exception {
        this.tableId = eventData.getTableId();
        this.database = eventData.getDatabase();
        this.table = eventData.getTable();
        ResultSetRowPacket[] results = client.query(new QueryCommand(String.format(QUERY_COLUME_NAME_SQL, this.database, this.table)));
        if (results.length == eventData.getColumnTypes().length) {
            columns = IntStream.range(0, results.length).mapToObj(i ->
                    new ColumnDesc(results[i].getValue(0), ColumnType.byCode(eventData.getColumnTypes()[i]))
            ).collect(Collectors.toList());
        } else {
            throw new Exception("colume count error");
        }
    }

    @Data
    @AllArgsConstructor
    @ToString
    public class ColumnDesc implements Serializable {

        private String columnName;

        private ColumnType columnType;
    }
}
