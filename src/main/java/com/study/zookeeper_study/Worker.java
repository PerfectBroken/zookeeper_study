package com.study.zookeeper_study;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Random;
import java.util.Scanner;

public class Worker implements Watcher {
    ZooKeeper zk;
    String hostPort = "10.1.133.138:2181";
    int timeout = 33000;
    String serverId = Integer.toHexString(new Random().nextInt());
    Logger log = Logger.getLogger(Master.class);
    String status = "";
    String name;

    Worker() {
    }

    void startZK() throws Exception {
        zk = new ZooKeeper(hostPort, timeout, this);
    }

    public void process(WatchedEvent e) {
        System.out.println(e);
    }

    public void stopZK() throws Exception {
        zk.close();
    }


    void register() throws Exception {

        log.info("注册从节点");
        zk.create("/workers/worker-" + serverId,
                serverId.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                workerCreateCallback,
                null);
    }

    public void setStatus(String status) {
        this.status = status;

    }

    public void updateStatus(String status) {
        if (status == this.status) {
            zk.setData("/workers/worker-" + serverId, status.getBytes(), -1, statusCallback, status);
        }
    }

    AsyncCallback.StatCallback statusCallback = new AsyncCallback.StatCallback()
    {
        public void processResult(int rc, String path, Object ctx, Stat name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    updateStatus((String) ctx);
                    break;
                default:
                    log.error("又出错了" + KeeperException.Code.get(rc) + "," + path);
            }
        }
    };

    AsyncCallback.StringCallback workerCreateCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            try {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS: {
                        register();
                        break;
                    }
                    case OK: {
                        log.info("我成功注册为小弟: worker-" + serverId);
                        break;
                    }
                    case NODEEXISTS: {
                        log.info("小弟worker-" + serverId + "已经注册过了");
                    }
                    default: {
                        log.error("好像出错了:" + KeeperException.Code.get(rc) + path);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    };

    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);
        final Worker w = new Worker();
        w.startZK();
        w.register();

        while (true) {
            int num = scanner.nextInt();
            switch (num) {
                case 0: {
                    w.stopZK();
                    break;
                }
                case 1: {
                    w.startZK();
                    w.register();
                    break;
                }
                case 2 : {
                    w.setStatus("busy");
                    break;
                }
                case 3 : {
                    w.setStatus("idle");

                }
            }
            Thread.sleep(2000);
        }
    }


}
