package org.github.luikia.replication;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.github.luikia.replication.format.ReplicationData;

public class NativeReplicationSourceFunction extends ReplicationSourceFunction<ReplicationData> {

    public NativeReplicationSourceFunction(String url, String username, String password, String soltName) {
        super(url, username, password, soltName);
    }

    @Override
    public ReplicationData format(ReplicationData data) {
        return data;
    }

    @Override
    public TypeInformation<ReplicationData> getProducedType() {
        return ReplicationData.TYPE;
    }
}
