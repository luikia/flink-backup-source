package org.github.luikia.oplog.client;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.bson.BsonDocument;
import org.bson.Document;
import org.github.luikia.oplog.format.ChangeStreamData;

import java.util.Objects;
import java.util.function.Consumer;

@Slf4j
public class ChangeStreamClient {

    private final MongoClient mongoClient;
    @Setter
    private String resumeToken;

    private volatile boolean running = false;

    public ChangeStreamClient(MongoClientSettings settings, String resumeToken) {
        this.mongoClient = MongoClients.create(settings);
        this.resumeToken = resumeToken;
    }

    public ChangeStreamClient(MongoClientSettings settings) {
        this(settings, null);
    }

    public void start(Consumer<ChangeStreamData> callback) throws Exception {
        ChangeStreamIterable<Document> watch = mongoClient.watch();
        if (StringUtils.isNotEmpty(resumeToken))
            watch.resumeAfter(BsonDocument.parse(String.format("{\"_data\": \"%s\"}", resumeToken)));
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = watch.cursor();
        running = true;
        while (running && cursor.hasNext()) {
            ChangeStreamDocument<Document> changeStream = cursor.next();
            if (Objects.isNull(changeStream))
                continue;
            OperationType operationType = changeStream.getOperationType();


            ChangeStreamData data = new ChangeStreamData();
            data.setCollection(changeStream.getNamespace().getCollectionName());
            data.setDb(changeStream.getDatabaseName());
            Integer time = changeStream.getClusterTime().getTime();
            data.setTs(time.longValue() * 1000);

            BsonDocument documentKey = changeStream.getDocumentKey();
            data.setDocumentKey(documentKey.toJson());
            Document fullDocument = changeStream.getFullDocument();
            if (operationType == OperationType.INSERT || operationType == OperationType.UPDATE || operationType == OperationType.REPLACE)
                if (Objects.nonNull(fullDocument))
                    data.setFullDocument(changeStream.getFullDocument().toJson());
                else
                    data.setFullDocument(queryMongoFromKey(data.getDb(), data.getCollection(), documentKey));
            data.setOperationType(changeStream.getOperationType().getValue());
            data.setResumeToken(changeStream.getResumeToken().get("_data").asString().getValue());
            callback.accept(data);
        }
        cursor.close();
        mongoClient.close();

    }

    public void close() {
        this.running = false;
    }

    private String queryMongoFromKey(String db, String collection, BsonDocument documentKey) {
        MongoCollection<Document> coll = mongoClient.getDatabase(db).getCollection(collection);
        FindIterable<Document> documents = coll.find(documentKey);
        Document first = documents.first();
        return Objects.nonNull(first) ? first.toJson() : null;
    }
}
