package org.github.luikia.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryNTimes;
import org.github.luikia.offset.Offset;

import java.io.Serializable;
import java.util.Objects;

public class ZkClient implements Serializable {

    private String url;

    private String path;

    private int retryTimes;

    private int sleepMsBetweenRetries;

    private transient CuratorFramework client;

    private transient InterProcessMutex lock;

    public ZkClient(String url, String path, int retryTimes, int sleepMsBetweenRetries) {
        this.url = url;
        this.path = path;
        this.retryTimes = retryTimes;
        this.sleepMsBetweenRetries = sleepMsBetweenRetries;
    }

    public void start() throws Exception {
        this.client = CuratorFrameworkFactory.newClient(url, new RetryNTimes(retryTimes, sleepMsBetweenRetries));
        this.client.start();
        if (Objects.isNull(client.checkExists().forPath(this.path))) {
            client.create().forPath(this.path);
        }
    }

    public InterProcessMutex getLock() {
        if(Objects.isNull(this.lock)){
           this.lock = new InterProcessMutex(this.client, path + "/lock");
        }
        return this.lock;
    }

    public void saveOffset(Offset offset) throws Exception {
        if (Objects.isNull(client.checkExists().forPath(this.path + "/offset"))) {
            client.create().forPath(this.path + "/offset");
        }
        client.setData().forPath(this.path + "/offset", offset.toJsonString().getBytes());
    }

    public String getOffsetJson(){
        try {
            byte[] b = client.getData().forPath(this.path + "/offset");
            return new String(b);
        } catch (Exception e) {
            return null;
        }
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public long getSleepMsBetweenRetries() {
        return sleepMsBetweenRetries;
    }

    public void setSleepMsBetweenRetries(int sleepMsBetweenRetries) {
        this.sleepMsBetweenRetries = sleepMsBetweenRetries;
    }
}
