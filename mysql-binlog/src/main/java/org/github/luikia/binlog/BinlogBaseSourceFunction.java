package org.github.luikia.binlog;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.github.luikia.BackupSourceFunction;
import org.github.luikia.binlog.format.BinLogRowData;
import org.github.luikia.binlog.offset.BinLogOffset;
import org.github.luikia.binlog.query.QueryClient;
import org.github.luikia.binlog.table.TableDesc;
import org.github.luikia.binlog.utils.BinLogConvertUtils;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@NoArgsConstructor
public abstract class BinlogBaseSourceFunction<T> extends BackupSourceFunction<T, BinLogOffset> {

    private static final long serialVersionUID = 1L;

    private transient BinaryLogClient client;

    private transient QueryClient queryClient;

    private transient Map<Long, TableDesc> tableDescMap;

    private String host;

    private int port;

    private String username;

    private String password;

    private BinLogOffset offset;

    private volatile boolean running = false;


    public BinlogBaseSourceFunction(String host, int port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;

    }

    @Override
    public void open(Configuration parameters) {
        tableDescMap = new ConcurrentHashMap(10);
        client = new BinaryLogClient(this.host, this.port, this.username, this.password);
        queryClient = new QueryClient(this.host, this.port, this.username, this.password);
        if (Objects.nonNull(this.offset)) {
            client.setBinlogFilename(this.offset.getBinlogFilename());
            client.setBinlogPosition(this.offset.getBinlogPosition());
        }
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
        );
        client.setEventDeserializer(eventDeserializer);
        this.startZKClient();

    }

    @Override
    public void run(SourceFunction.SourceContext<T> ctx) throws Exception {
        client.registerEventListener(event -> {
            final Object lock = ctx.getCheckpointLock();
            if (event.getHeader().getEventType() == EventType.TABLE_MAP) {
                TableMapEventData eventData = event.getData();
                if (!tableDescMap.containsKey(eventData.getTableId()))
                    try {
                        TableDesc desc = new TableDesc(event.getData(), BinlogBaseSourceFunction.this.queryClient);
                        tableDescMap.put(desc.getTableId(), desc);
                    } catch (Exception e) {
                        log.error("table mapping error", e);
                    }
            } else if (event.getHeader().getEventType() == EventType.ROTATE) {
                synchronized (lock) {
                    RotateEventData eventData = event.getData();
                    this.offset = BinLogOffset.of(eventData);
                }
                return;
            }
            EventHeaderV4 headerV4 = event.getHeader();
            synchronized (lock) {
                BinLogRowData binLogRowData = BinLogConvertUtils.convertRowData(event, tableDescMap);
                if (Objects.nonNull(binLogRowData)) {
                    ctx.collect(BinlogBaseSourceFunction.this.format(binLogRowData));
                }
                if (headerV4.getNextPosition() != 0) {
                    this.offset.setBinlogPosition(headerV4.getNextPosition());
                }
            }

        });
        if (this.getLock()) {
            running = true;
            if (Objects.isNull(this.offset) && Objects.nonNull(zkClient)) {
                this.offset = BinLogOffset.fromJson(zkClient.getOffsetJson());
            }
            if (Objects.nonNull(this.offset)) {
                client.setBinlogFilename(this.offset.getBinlogFilename());
                client.setBinlogPosition(this.offset.getBinlogPosition());
            }
            queryClient.connect();
            client.connect();
        }

    }

    @Override
    public void cancel() {
        try {
            queryClient.disconnect();
            client.disconnect();
            if (Objects.nonNull(this.zkClient) && running) {
                this.zkClient.getLock().release();
            }
            running = false;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public abstract T format(BinLogRowData data);

    @Override
    public BinLogOffset getOffset() {
        return offset;
    }

    @Override
    public void setOffset(BinLogOffset offset) {
        this.offset = offset;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public BinLogOffset formJson(String json) {
        return BinLogOffset.fromJson(json);
    }
}
