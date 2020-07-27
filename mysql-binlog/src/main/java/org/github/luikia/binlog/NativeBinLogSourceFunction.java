package org.github.luikia.binlog;


import org.github.luikia.binlog.format.BinLogRowData;

public class NativeBinLogSourceFunction extends BinlogBaseSourceFunction<BinLogRowData> {

    private static final long serialVersionUID = 1L;

    public NativeBinLogSourceFunction(String id, String host, int port, String username, String password) {
        super(host, port, username, password);
    }

    @Override
    public BinLogRowData format(BinLogRowData data) {
        return data;
    }
}
