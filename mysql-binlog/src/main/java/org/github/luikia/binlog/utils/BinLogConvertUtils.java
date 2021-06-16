package org.github.luikia.binlog.utils;

import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import org.github.luikia.binlog.format.BinLogRowData;
import org.github.luikia.binlog.table.TableDesc;
import org.github.luikia.type.LogType;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BinLogConvertUtils {

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static BinLogRowData onInsert(WriteRowsEventData data, long timestamp, Map<Long, TableDesc> tableMeta) {
        return onInsertAndDelete(LogType.INSERT, data.getTableId(), data.getRows(), timestamp, tableMeta);
    }

    private static BinLogRowData onDelete(DeleteRowsEventData data, long timestamp, Map<Long, TableDesc> tableMeta) {
        return onInsertAndDelete(LogType.DELETE, data.getTableId(), data.getRows(), timestamp, tableMeta);
    }

    private static BinLogRowData onUpdate(UpdateRowsEventData data, long timestamp, Map<Long, TableDesc> tableMeta) {
        TableDesc desc = tableMeta.get(data.getTableId());
        BinLogRowData rowData = initRowData(LogType.UPDATE, timestamp, desc);
        List<TableDesc.ColumnDesc> columns = desc.getColumns();
        List<BinLogRowData.RowData> rows =
                data.getRows().stream().map(row -> {
                    BinLogRowData.RowData r = new BinLogRowData.RowData();
                    Map<String, Serializable> map_new = new LinkedHashMap<>(columns.size());
                    Map<String, Serializable> map_old = new LinkedHashMap<>(columns.size());
                    IntStream.range(0, columns.size()).forEach(i -> {
                        String colName = columns.get(i).getColumnName();
                        Serializable cell_new = row.getValue()[i];
                        Serializable cell_old = row.getKey()[i];
                        ColumnType type = columns.get(i).getColumnType();
                        map_new.put(colName, convertType(cell_new, type));
                        map_old.put(colName, convertType(cell_old, type));
                    });
                    r.setData(map_new);
                    r.setOld(map_old);
                    return r;
                }).collect(Collectors.toList());
        rowData.setRows(rows);
        return rowData;
    }


    private static BinLogRowData onInsertAndDelete(LogType type, long tableId,
                                                   List<Serializable[]> rows,
                                                   long timestamp, Map<Long, TableDesc> tableMeta) {
        TableDesc desc = tableMeta.get(tableId);
        BinLogRowData rowData = initRowData(type, timestamp, desc);
        List<TableDesc.ColumnDesc> columns = desc.getColumns();
        List<BinLogRowData.RowData> rowList =
                rows.stream().map(row -> {
                    BinLogRowData.RowData r = new BinLogRowData.RowData();
                    Map<String, Serializable> map = new LinkedHashMap<>(columns.size());
                    IntStream.range(0, columns.size()).forEach(i ->
                            map.put(columns.get(i).getColumnName(),
                                    convertType(row[i], columns.get(i).getColumnType()))
                    );
                    r.setData(map);
                    return r;
                }).collect(Collectors.toList());
        rowData.setRows(rowList);
        return rowData;
    }

    private static BinLogRowData initRowData(LogType type, long timestamp, TableDesc desc) {
        BinLogRowData rowData = new BinLogRowData();
        rowData.setDatabase(desc.getDatabase());
        rowData.setTable(desc.getTable());
        rowData.setTimestamp(timestamp);
        rowData.setType(type);
        return rowData;
    }

    public static BinLogRowData convertRowData(Event e, Map<Long, TableDesc> tableMeta) {
        EventType type = e.getHeader().getEventType();
        long timestamp = e.getHeader().getTimestamp();
        BinLogRowData t = null;
        switch (type) {
            case EXT_WRITE_ROWS:
            case WRITE_ROWS:
                t = onInsert(e.getData(), timestamp, tableMeta);
                break;
            case EXT_UPDATE_ROWS:
            case UPDATE_ROWS:
                t = onUpdate(e.getData(), timestamp, tableMeta);
                break;
            case EXT_DELETE_ROWS:
            case DELETE_ROWS:
                t = onDelete(e.getData(), timestamp, tableMeta);
                break;
            default:
                break;
        }
        return t;

    }


    private static Serializable convertType(Serializable data, ColumnType type) {
        if (Objects.isNull(data))
            return null;
        if (type == ColumnType.LONG)
            return data;
        else if (type == ColumnType.VARCHAR || type == ColumnType.VAR_STRING || type == ColumnType.STRING)
            if (data instanceof byte[]) {
                byte[] b = (byte[]) data;
                return new String(b);
            } else
                return null;
        else if (type == ColumnType.DATE
                || type == ColumnType.DATETIME || type == ColumnType.DATETIME_V2
                || type == ColumnType.TIME || type == ColumnType.TIME_V2
                || type == ColumnType.TIMESTAMP || type == ColumnType.TIMESTAMP_V2)
            if (data instanceof Long)
                return LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) data), ZoneId.systemDefault()).format(formatter);
            else
                return null;
        else
            return data;
    }
}
