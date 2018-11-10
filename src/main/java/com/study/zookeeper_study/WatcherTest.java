package com.study.zookeeper_study;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;


public class WatcherTest implements Watcher {

    WatcherTest(){
        
    }

    ZooKeeper zk;
    public void process(WatchedEvent e) {
        System.out.println(
                "Watcher1" + e.toString()
        );
    }

    private void checkMaster() {
        zk.exists(
                "/master",
                watcher2,
                masterCallBack,
                null
        );
    }

    AsyncCallback.StatCallback masterCallBack = new AsyncCallback.StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    break;
                case OK:
                    break;
                default:
                    break;
            }
        }
    };

    Watcher watcher2 = new Watcher() {
        public void process(WatchedEvent event) {
            System.out.println(
                    "Watcher2" + event.toString()
            );
        }
    };

}
