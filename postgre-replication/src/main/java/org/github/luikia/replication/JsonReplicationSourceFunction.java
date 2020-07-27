package org.github.luikia.replication;

import com.google.gson.Gson;
import org.github.luikia.replication.format.ReplicationData;

public class JsonReplicationSourceFunction extends ReplicationSourceFunction<String> {

    private static final long serialVersionUID = 1L;

    private static final Gson g = new Gson();

    public JsonReplicationSourceFunction(String url, String username, String password, String soltName) {
        super(url, username, password, soltName);
    }

    @Override
    public String format(ReplicationData data) {
        return g.toJson(data, ReplicationData.class);
    }
}
