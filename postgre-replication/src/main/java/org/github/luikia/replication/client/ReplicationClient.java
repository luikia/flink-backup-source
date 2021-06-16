package org.github.luikia.replication.client;

import lombok.extern.slf4j.Slf4j;
import org.github.luikia.replication.format.ReplicationData;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;

import java.nio.ByteBuffer;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Slf4j
public class ReplicationClient {

    private String url;

    private String username;

    private String password;

    private String soltName;

    private String minVersion;

    private LogSequenceNumber startLsn;

    private Thread replicationThread;

    private Consumer<ReplicationData> callBack;

    private volatile boolean running = false;

    public ReplicationClient(String url, String username, String password, String soltName) {
        this(url, username, password, soltName, "9.4");
    }

    public ReplicationClient(String url, String username, String password, String soltName, String minVersion) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.soltName = soltName;
        this.minVersion = minVersion;
    }

    public void start() throws SQLException, InterruptedException {
        PGConnection conn = DriverManager
                .getConnection(url, this.getConnectProperties())
                .unwrap(PGConnection.class);

        ChainedLogicalStreamBuilder builder = conn.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(soltName)
                .withSlotOption("include-timestamp", true)
                .withSlotOption("include-lsn", true);
        if (Objects.nonNull(this.startLsn))
            builder = builder.withStartPosition(this.startLsn);
        PGReplicationStream stream = builder.start();
        this.replicationThread = new Thread(new ReplicatrionListener(stream, callBack));
        this.replicationThread.start();
        this.replicationThread.join();
    }


    public boolean isRunning() {
        return this.running;
    }

    public void close() throws InterruptedException {
        this.running = false;
        replicationThread.join(10000L);
    }

    private Properties getConnectProperties() {
        Properties props = new Properties();
        PGProperty.USER.set(props, this.username);
        PGProperty.PASSWORD.set(props, this.password);
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, this.minVersion);
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");
        return props;
    }

    private class ReplicatrionListener implements Runnable {
        PGReplicationStream stream;
        Consumer<ReplicationData> callBack;

        private ReplicatrionListener(PGReplicationStream stream, Consumer<ReplicationData> callBack) {
            this.stream = stream;
            this.callBack = callBack;
        }


        @Override
        public void run() {
            ReplicationClient.this.running = true;
            while (running)
                try {
                    ByteBuffer msg = stream.readPending();
                    ReplicationData data = new ReplicationData(stream.getLastReceiveLSN(), msg);
                    this.callBack.accept(data);
                    if (Objects.isNull(data.getChanges())) {
                        Thread.sleep(TimeUnit.MILLISECONDS.toMillis(200L));
                    }
                } catch (Exception ex) {
                    log.error("get replication data error", ex);
                }
            try {
                stream.close();
            } catch (SQLException e) {
                log.error("close stream error", e);
            }

        }
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSoltName() {
        return soltName;
    }

    public void setSoltName(String soltName) {
        this.soltName = soltName;
    }

    public String getMinVersion() {
        return minVersion;
    }

    public void setMinVersion(String minVersion) {
        this.minVersion = minVersion;
    }

    public LogSequenceNumber getStartLsn() {
        return startLsn;
    }

    public void setStartLsn(LogSequenceNumber startLsn) {
        this.startLsn = startLsn;
    }

    public void setCallBack(Consumer<ReplicationData> callBack) {
        this.callBack = callBack;
    }
}
