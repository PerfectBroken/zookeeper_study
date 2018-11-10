package com.study.zookeeper_study;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.Random;
import java.util.Scanner;

public class Master implements Watcher {
    ZooKeeper zk;
    String hostPort = "10.1.133.138:2181";
    int timeout = 33000;
    String serverId = Integer.toHexString(new Random().nextInt());
    boolean isLeader = false;
    Logger log = Logger.getLogger(Master.class);

    Master(){
    }

    void startZK() throws Exception{
        zk = new ZooKeeper(hostPort, timeout, this);
    }

    public void process(WatchedEvent e){
        System.out.println(e);
    }

    public void stopZK() throws Exception{
        zk.close();
    }

    boolean checkMaster(){
        while(true){
            try{
                Stat stat = new Stat();
                byte data[] = zk.getData("/master", false, stat);
                isLeader = new String(data).equals(serverId);
                if(!isLeader){

                }
                return true;
            }
            catch (KeeperException.NodeExistsException e){
                isLeader = false;
                break;
            }
            catch (Exception e){

            }
        }
        return false;
    }

    void runForMaster(){
        while(true){
            try{
                log.info("我去竞选");
                zk.create("/master",
                        serverId.getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL,
                        masterCreateCallback,
                        null);
                isLeader = true;
                if(isLeader) break;
            }
            catch (Exception e){
            }
            if(checkMaster()) break;
            try {
                Thread.sleep(1000);
            }
            catch (Exception e){}
        }
    }

    public void bootstrap() throws Exception{
        while (true) {
            if (isLeader) {
                log.info("创建父节点");
                createParent("/tasks",new byte[0]);
                createParent("/assign",new byte[0]);
                createParent("/workers",new byte[0]);
                createParent("/status",new byte[0]);
                break;
            }
            Thread.sleep(1000);
        }
    }

    void getMasterNode() throws Exception{
        Stat stat = new Stat();
        byte data[] = zk.getData("/master", false, stat);
        log.info(new String(data) + "是老大");
    }

    void createParent(String path, byte[] data){
        zk.create(path,
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                createParentCallback,
                data);
    }

    AsyncCallback.StringCallback createParentCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    createParent(path, (byte[]) ctx);
                    break;
                case OK:
                    log.info(path + "创建成功");
                    break;
                case NODEEXISTS:
                    log.warn(path + "已经存在了");
                    break;
                default:
                    log.error("好像出错了:" + KeeperException.Code.get(rc) + path);

            }
        }
    };

    AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:{
                    //链接丢失的情况下,检查master是否存在.
                    checkMaster();
                    return;
                }
                case OK:{
                    isLeader = true;
                    //创建成功 开始行使领导权
                    //takeLeadership()
                    break;
                }
                case NODEEXISTS:{

                }
                default:{
                    isLeader = false;
                }
            }
            log.info(isLeader ? "我当了老大" : "我还是小弟");
        }
    };

    void masterExists(){
        zk.exists("/master",
                masterExistWatcher,
                masterExistsCallback,
                null);
    }

    Watcher masterExistWatcher = new Watcher() {
        public void process(WatchedEvent watchedEvent) {
            if(watchedEvent.getType() == Event.EventType.NodeDeleted){
                assert "/master".equals(watchedEvent.getPath());
                try {
                    runForMaster();
                }
                catch (Exception e){
                    log.error(e.getMessage());
                }
            }
        }
    };

    AsyncCallback.StatCallback masterExistsCallback = new AsyncCallback.StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    try{
                        Thread.sleep(1000);
                    }
                    catch (Exception e){}
                    masterExists();
                case OK:
                    runForMaster();
//                    stat = MasterStates.RUNNING;
                default:

            }
        }
    };

    Watcher workerChangeWathcer = new Watcher() {
        public void process(WatchedEvent watchedEvent) {
            assert "/workers".equals(watchedEvent.getPath());

            getWorkers();
        }
    };

    void getWorkers(){
        zk.getChildren("/workers",
                workerChangeWathcer,
                workersCallback,
                null
                );
    }

    AsyncCallback.ChildrenCallback workersCallback = new AsyncCallback.ChildrenCallback(){
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    getWorkers();
                    break;
                case OK:
                    log.info("获取从节点" + children.size() + "个");
                    for(String s : children){
                        log.info(s);
                    }
                    break;
                default:
                    log.error("错误");
            }
        }
    };


    public static void main(String[]  args) throws Exception{
        Scanner scanner = new Scanner(System.in);
        final Master m = new Master();
        m.startZK();

        Thread thread = new Thread(new Runnable() {
            public void run() {
                try {
                    m.runForMaster();
                }
                catch (Exception e){}
            }
        });
        thread.run();
        m.bootstrap();

        System.out.println(m.serverId);
        for(int i = 0 ;i < m.serverId.getBytes().length; i++){
            System.out.print(m.serverId.getBytes()[i]);
        }
        System.out.println();

        m.getWorkers();

        while(true){
            int num = scanner.nextInt();
            switch (num){
                case 0 :{
                    m.stopZK();
                    break;
                }
                case 1:{
                    m.startZK();
                    break;
                }
                case 2:{
                    Thread thread2 = new Thread(new Runnable() {
                        public void run() {
                            try {
                                m.runForMaster();
                            }
                            catch (Exception e){}
                        }
                    });
                    thread2.run();
                    break;
                }
                case 3:{
                    m.getMasterNode();
                }
                case 4:{
                    m.getWorkers();
                }
            }
            Thread.sleep(2000);
        }
    }


}
