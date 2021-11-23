package org.github.luikia.binlog.query;

import com.github.shyiko.mysql.binlog.network.*;
import com.github.shyiko.mysql.binlog.network.protocol.ErrorPacket;
import com.github.shyiko.mysql.binlog.network.protocol.GreetingPacket;
import com.github.shyiko.mysql.binlog.network.protocol.PacketChannel;
import com.github.shyiko.mysql.binlog.network.protocol.ResultSetRowPacket;
import com.github.shyiko.mysql.binlog.network.protocol.command.PingCommand;
import com.github.shyiko.mysql.binlog.network.protocol.command.QueryCommand;
import com.github.shyiko.mysql.binlog.network.protocol.command.SSLRequestCommand;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Slf4j
public class QueryClient {

    private static final SSLSocketFactory DEFAULT_REQUIRED_SSL_MODE_SOCKET_FACTORY = new DefaultSSLSocketFactory() {

        @Override
        protected void initSSLContext(SSLContext sc) throws GeneralSecurityException {
            sc.init(null, new TrustManager[]{
                    new X509TrustManager() {

                        @Override
                        public void checkClientTrusted(X509Certificate[] x509Certificates, String s)
                                throws CertificateException {
                        }

                        @Override
                        public void checkServerTrusted(X509Certificate[] x509Certificates, String s)
                                throws CertificateException {
                        }

                        @Override
                        public X509Certificate[] getAcceptedIssuers() {
                            return new X509Certificate[0];
                        }
                    }
            }, null);
        }
    };
    private static final SSLSocketFactory DEFAULT_VERIFY_CA_SSL_MODE_SOCKET_FACTORY = new DefaultSSLSocketFactory();


    private PacketChannel channel;
    private final String host;
    private final int port;
    private final String username;
    private final String password;
    @Setter
    private SSLMode sslMode = SSLMode.DISABLED;


    private final Thread heartbeatThread;

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
        tryUpgradeToSSL(packet);
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
        if (this.heartbeatThread.isAlive())
            this.heartbeatThread.interrupt();
    }


    private void authenticate(GreetingPacket greetingPacket) throws IOException {
        new Authenticator(greetingPacket, channel, "", this.username, this.password).authenticate();
        this.channel.authenticationComplete();
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
        List<ResultSetRowPacket> resultSet = new LinkedList<>();
        byte[] statementResult = channel.read();
        if (statementResult[0] == (byte) 0xFF /* error */) {
            byte[] bytes = Arrays.copyOfRange(statementResult, 1, statementResult.length);
            ErrorPacket errorPacket = new ErrorPacket(bytes);
            throw new ServerException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(),
                    errorPacket.getSqlState());
        }
        while ((channel.read())[0] != (byte) 0xFE /* eof */) { /* skip */ }
        for (byte[] bytes; (bytes = channel.read())[0] != (byte) 0xFE /* eof */; )
            resultSet.add(new ResultSetRowPacket(bytes));
        return resultSet.toArray(new ResultSetRowPacket[resultSet.size()]);
    }

    private boolean tryUpgradeToSSL(GreetingPacket greetingPacket) throws IOException {
        int collation = greetingPacket.getServerCollation();

        if (sslMode != SSLMode.DISABLED) {
            boolean serverSupportsSSL = (greetingPacket.getServerCapabilities() & ClientCapabilities.SSL) != 0;
            if (!serverSupportsSSL && (sslMode == SSLMode.REQUIRED || sslMode == SSLMode.VERIFY_CA ||
                    sslMode == SSLMode.VERIFY_IDENTITY)) {
                throw new IOException("MySQL server does not support SSL");
            }
            if (serverSupportsSSL) {
                SSLRequestCommand sslRequestCommand = new SSLRequestCommand();
                sslRequestCommand.setCollation(collation);
                channel.write(sslRequestCommand);
                SSLSocketFactory sslSocketFactory =
                        sslMode == SSLMode.REQUIRED || sslMode == SSLMode.PREFERRED ?
                                DEFAULT_REQUIRED_SSL_MODE_SOCKET_FACTORY :
                                DEFAULT_VERIFY_CA_SSL_MODE_SOCKET_FACTORY;
                channel.upgradeToSSL(sslSocketFactory,
                        sslMode == SSLMode.VERIFY_IDENTITY ? new TLSHostnameVerifier() : null);
                log.info("SSL enabled");
                return true;
            }
        }
        return false;
    }

}
