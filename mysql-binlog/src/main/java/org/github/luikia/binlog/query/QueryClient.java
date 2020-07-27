package org.github.luikia.binlog.query;

import com.github.shyiko.mysql.binlog.network.AuthenticationException;
import com.github.shyiko.mysql.binlog.network.ServerException;
import com.github.shyiko.mysql.binlog.network.protocol.ErrorPacket;
import com.github.shyiko.mysql.binlog.network.protocol.GreetingPacket;
import com.github.shyiko.mysql.binlog.network.protocol.PacketChannel;
import com.github.shyiko.mysql.binlog.network.protocol.ResultSetRowPacket;
import com.github.shyiko.mysql.binlog.network.protocol.command.AuthenticateCommand;
import com.github.shyiko.mysql.binlog.network.protocol.command.PingCommand;
import com.github.shyiko.mysql.binlog.network.protocol.command.QueryCommand;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class QueryClient {

    private PacketChannel channel;
    private String host;
    private int port;
    private String username;
    private String password;

    private Thread heartbeatThread;

    public QueryClient(String host, int port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        heartbeatThread = new Thread(() -> {
            while (true)
                try {
                    Thread.sleep(TimeUnit.MINUTES.toMillis(1));
                    this.channel.write(new PingCommand());
                } catch (InterruptedException e) {
                    return;
                } catch (Exception e) {
                    e.printStackTrace();
                }
        });
    }

    public void connect() throws IOException {
        reconnect();
    }

    private void reconnect() throws IOException {
        this.disconnect();
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(this.host, this.port), 1000);
        this.channel = new PacketChannel(socket);
        GreetingPacket packet = receiveGreeting();
        authenticate(packet);
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();

    }

    public ResultSetRowPacket[] query(QueryCommand command) throws IOException {
        if (!this.channel.isOpen())
            reconnect();
        try {
            this.channel.write(command);
        } catch (SocketException ex) {
            reconnect();
            this.channel.write(command);
        }

        return readResultSet();
    }


    public boolean isConnected() {
        return this.channel.isOpen();
    }

    public void disconnect() throws IOException {
        if (Objects.nonNull(this.channel))
            channel.close();
        if (this.heartbeatThread.isAlive()) {
            this.heartbeatThread.interrupt();
        }

    }


    private void authenticate(GreetingPacket greetingPacket) throws IOException {
        int collation = greetingPacket.getServerCollation();
        int packetNumber = 1;
        AuthenticateCommand authenticateCommand = new AuthenticateCommand("", this.username, this.password,
                greetingPacket.getScramble());
        authenticateCommand.setCollation(collation);
        this.channel.write(authenticateCommand, packetNumber);
        byte[] authenticationResult = this.channel.read();
        if (authenticationResult[0] != (byte) 0x00 /* ok */) {
            if (authenticationResult[0] == (byte) 0xFF /* error */) {
                byte[] bytes = Arrays.copyOfRange(authenticationResult, 1, authenticationResult.length);
                ErrorPacket errorPacket = new ErrorPacket(bytes);
                throw new AuthenticationException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(),
                        errorPacket.getSqlState());
            } else {
                throw new AuthenticationException("Unexpected authentication result (" + authenticationResult[0] + ")");
            }
        }
    }

    private GreetingPacket receiveGreeting() throws IOException {
        byte[] initialHandshakePacket = channel.read();
        if (initialHandshakePacket[0] == (byte) 0xFF /* error */) {
            byte[] bytes = Arrays.copyOfRange(initialHandshakePacket, 1, initialHandshakePacket.length);
            ErrorPacket errorPacket = new ErrorPacket(bytes);
            throw new ServerException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(),
                    errorPacket.getSqlState());
        }
        return new GreetingPacket(initialHandshakePacket);
    }

    private ResultSetRowPacket[] readResultSet() throws IOException {
        List<ResultSetRowPacket> resultSet = new LinkedList<ResultSetRowPacket>();
        byte[] statementResult = channel.read();
        if (statementResult[0] == (byte) 0xFF /* error */) {
            byte[] bytes = Arrays.copyOfRange(statementResult, 1, statementResult.length);
            ErrorPacket errorPacket = new ErrorPacket(bytes);
            throw new ServerException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(),
                    errorPacket.getSqlState());
        }
        while ((channel.read())[0] != (byte) 0xFE /* eof */) { /* skip */ }
        for (byte[] bytes; (bytes = channel.read())[0] != (byte) 0xFE /* eof */; ) {
            resultSet.add(new ResultSetRowPacket(bytes));
        }
        return resultSet.toArray(new ResultSetRowPacket[resultSet.size()]);
    }

}
