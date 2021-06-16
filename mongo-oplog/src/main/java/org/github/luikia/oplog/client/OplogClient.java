package org.github.luikia.oplog.client;

import com.mongodb.CursorType;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.github.luikia.oplog.format.OplogData;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

@Slf4j
public class OplogClient {

    private static final List<BsonDocument> BASE_FILTERS = Arrays.asList(
            BsonDocument.parse("{\"op\":{\"$in\":[\"i\",\"u\",\"d\"]}}"),
            BsonDocument.parse("{\"ns\":{\"$not\":/admin.*/}}"),
            BsonDocument.parse("{\"ns\":{\"$not\":/local.*/}}"),
            BsonDocument.parse("{\"ns\":{\"$not\":/config.*/}}")
            //BsonDocument.parse("{\"ns\":{\"$and\":[{\"$not\":/admin.*/},{\"$not\":/local.*/},{\"$not\":/config.*/}]}}")
    );

    private final MongoClient mongoClient;
    private Long offset;
    private Consumer<OplogData> callback;
    private volatile MongoCursor<Document> resultCursor;
    private Thread opLogThread;

    private volatile boolean running = false;

    public OplogClient(MongoClientSettings settings, Long offset) {
        this.mongoClient = MongoClients.create(settings);
        this.offset = offset;
    }

    public OplogClient(MongoClientSettings settings) {
        this(settings, null);
    }

    public void start() throws Exception {
        MongoCollection<Document> oplogDocument = getOpLogCollection();
        if (Objects.isNull(oplogDocument)) {
            throw new Exception("server oplog config error,not find oplog document");
        }
        BsonTimestamp currentTS = Objects.isNull(offset) ? new BsonTimestamp((int) (System.currentTimeMillis() / 1000), NumberUtils.INTEGER_ZERO) : new BsonTimestamp(offset);
        this.resultCursor = oplogDocument
                .find(query(currentTS))
                .batchSize(100).noCursorTimeout(true)
                .cursorType(CursorType.Tailable).cursor();
        opLogThread = new Thread(new OplogListener(currentTS));
        opLogThread.start();
        opLogThread.join();
    }

    public void close() throws InterruptedException {
        this.running = false;
        if (Objects.nonNull(this.resultCursor)) {
            resultCursor.close();
            mongoClient.close();
            opLogThread.join(10000L);
        }
    }

    private MongoCollection<Document> getOpLogCollection() {
        MongoDatabase db = mongoClient.getDatabase("local");
        Optional<String> nameOp = StreamSupport.stream(db.listCollectionNames().spliterator(), false)
                .filter(s -> StringUtils.startsWithIgnoreCase(s, "oplog")).findFirst();
        return nameOp.map(db::getCollection).orElse(null);
    }

    private BsonDocument query(BsonTimestamp ts) {
        BsonArray filters = new BsonArray();
        filters.add(new BsonDocument().append("ts", new BsonDocument().append("$gt", ts)));
        filters.addAll(BASE_FILTERS);
        return new BsonDocument().append("$and", filters);
    }

    public void setCallback(Consumer<OplogData> callback) {
        this.callback = callback;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public boolean isRunning() {
        return running;
    }

    private class OplogListener implements Runnable {

        private BsonTimestamp currentTS;

        private OplogListener(BsonTimestamp currentTS) {
            this.currentTS = currentTS;
        }

        @Override
        public void run() {
            OplogClient.this.running = true;
            while (OplogClient.this.running) {
                try {
                    if (resultCursor.hasNext()) {
                        long maxTS = currentTS.getValue();
                        OplogData data = new OplogData(resultCursor.next());
                        maxTS = Math.max(data.getOffset(), maxTS);
                        currentTS = new BsonTimestamp(maxTS);
                        if (Objects.nonNull(OplogClient.this.callback))
                            try {
                                OplogClient.this.callback.accept(data);
                            } catch (Exception ex) {
                                log.error("handle oplog data error", ex);
                            }
                    } else
                        OplogClient.this.running = false;
                } catch (Exception e) {
                    if (OplogClient.this.running)
                        log.error("oplog listen error", e);
                    else
                        log.debug("oplog client close");
                }
            }
        }
    }
}
