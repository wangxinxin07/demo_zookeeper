package com.wxx.demo;

import com.alibaba.fastjson.JSON;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Api.java
 *
 * @author wangxinxin07
 * @date 2019/7/13
 */
public class Api {

    public static final Logger LOGGER = LoggerFactory.getLogger(Api.class);
    private static CountDownLatch connectionCountDownLantch = new CountDownLatch(1);

    private static ZooKeeper zooKeeper = null;

    public static void main(String[] args) throws Exception {
//        createConnection();

//        ZooKeeper connection = getConnection();
//        createZNodeSync(connection);
//        createZNodeAsync(connection);

//        deleteZNodeSync();
//        deleteZNodeAsync();


//        getChildrenSync();
        getChildrenAsync();

    }

    /**
     * 异步获取节点值  使用场景：应用启动过程中，获取配置信息；若配置信息较大，不希望获取配置信息步骤影响应用的主进程
     *
     * @throws Exception
     */
    private static void getChildrenAsync() throws Exception {
        ZooKeeper connection = getConnection();

        //创建父节点
        String path = connection.create("/test002", "i am father".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        LOGGER.info("father path = {}", path);

        String childPath1 = connection.create(path + "/child001", "i am child001".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        LOGGER.info("childPath1 = {}", childPath1);

        connection.getChildren(path, true, new ChildrenGetCallBack(), "I AM CTX");

        Thread.sleep(Integer.MAX_VALUE);
    }

    static class ChildrenGetCallBack implements AsyncCallback.ChildrenCallback{

        @Override
        public void processResult(int rc, String path, Object ctx, List<String> childrenList) {
            LOGGER.info("*******GET CHILDREN START********");
            LOGGER.info("RC = {}", rc);
            LOGGER.info("path = {}", path);
            LOGGER.info("ctx = {}", ctx);
            LOGGER.info("childrenList = {}", JSON.toJSONString(childrenList));
            LOGGER.info("*******GET CHILDREN END********");
        }
    }

    /**
     * 同步获取子节点数据
     */
    private static void getChildrenSync() throws Exception {
        ZooKeeper connection = getConnection();
        //创建父节点
        String path = connection.create("/test001", "i am test001".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        LOGGER.info("path = {}", path);

        //创建子节点
        String path2 = connection.create(path + "/child001", "i am test001 child 001".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        LOGGER.info("path2 = {}", path2);

        List<String> children = connection.getChildren(path, true);
        LOGGER.info("children = {}", JSON.toJSONString(children));

        //继续创建子节点
        String path3 = connection.create(path + "/child002", "i am test001 child 002".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * 异步删除节点
     */
    private static void deleteZNodeAsync() throws Exception {
        ZooKeeper connection = getConnection();
        connection.delete("/zk-async-persistent-node", 0, new DeleteCallBack(), "i am ctx");
        Thread.sleep(Integer.MAX_VALUE);
    }

    static class DeleteCallBack implements AsyncCallback.VoidCallback {

        @Override
        public void processResult(int rc, String s, Object ctx) {
            LOGGER.info("DELETE START -----------------");
            LOGGER.info("RC = {}", rc);
            LOGGER.info("s = {}", s);
            LOGGER.info("ctx = {}", ctx);
            LOGGER.info("DELETE END -----------------");
        }
    }


    /**
     * 同步删除节点
     *
     * @throws Exception
     */
    private static void deleteZNodeSync() throws Exception {
        ZooKeeper connection = getConnection();
        connection.delete("/zk-async-persistent-node0000000040", 0);
    }


    /**
     * 异步方式创建节点
     *
     * @param connection
     */
    private static void createZNodeAsync(ZooKeeper connection) throws Exception {
        connection.create("/zk-async-temp-node", "异步创建临时节点".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, new CallBackClass(), "i am context");
        connection.create("/zk-async-temp-node", "异步创建临时顺序节点".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL, new CallBackClass(), "i am context");
        connection.create("/zk-async-persistent-node", "异步创建持久节点".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new CallBackClass(), "i am context");
        connection.create("/zk-async-persistent-node", "异步创建持久顺序节点".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL, new CallBackClass(), "i am context");

        Thread.sleep(Integer.MAX_VALUE);
    }

    static class CallBackClass implements AsyncCallback.StringCallback {

      /*  @Override
        public void processResult(int rc, String path, Object ctx, List<ACL> list, Stat stat) {
            LOGGER.info("RC = {}", rc);
            LOGGER.info("path = {}", path);
            LOGGER.info("ctx = {}", ctx);
            LOGGER.info("acl list = {}", JSON.toJSONString(list));
            LOGGER.info("stat = {}", stat);
        }*/

        /**
         * @param rc   响应状态码
         * @param path 接口调用时传入的参数路径
         * @param ctx  ctx参数
         * @param name 实际创建的path全路径
         */
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            LOGGER.info("--------------------------");
            LOGGER.info("RC = {}", rc);
            LOGGER.info("path = {}", path);
            LOGGER.info("ctx = {}", ctx);
            LOGGER.info("name = {}", name);
            LOGGER.info("--------------------------");
        }
    }

    /**
     * 同步创建节点
     *
     * @param connection
     * @throws Exception
     */
    private static void createZNodeSync(ZooKeeper connection) throws Exception {
        //持久顺序节点
        String path1 = connection.create("/test005", "hello world".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        LOGGER.info("PATH1 = {}", path1);

        //持久节点
        String path2 = connection.create("/test005", "hello world".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        LOGGER.info("path2 = {}", path2);

        //临时节点
        String path3 = connection.create("/test004_temp", "i am 临时节点".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        LOGGER.info("path3 = {}", path3);

        //临时顺序节点
        String path4 = connection.create("/test004_temp", "i am 临时节点".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        LOGGER.info("path4 = {}", path4);

    }


    /**
     * 创建连接
     * 由于建立连接是异步的，使用countdownLatch进行同步
     */
    public static void createConnection() {
        try {
            ZooKeeper zooKeeper = new ZooKeeper(Config.CONNECT_STRING, Config.SESSION_TIMEOUT, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                        connectionCountDownLantch.countDown();
                        LOGGER.info("建立连接成功~");
                    }
                }
            });
            LOGGER.info("zooKeeper.getState1 = {}", zooKeeper.getState());
            connectionCountDownLantch.await();
            LOGGER.info("zooKeeper.getState2 = {}", zooKeeper.getState());
            zooKeeper.close();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage(), e);
        }

    }


    /**
     * 获取到连接
     *
     * @return
     */
    public static ZooKeeper getConnection() {
        try {
            zooKeeper = new ZooKeeper(Config.CONNECT_STRING, Config.SESSION_TIMEOUT, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    if (watchedEvent.getState() == Event.KeeperState.SyncConnected && watchedEvent.getType() == Event.EventType.None) {
                        connectionCountDownLantch.countDown();
                        LOGGER.info("建立连接成功~");
                    } else if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                        //子节点变更通知事件
                        try {
                            //watch 为false只能监听一次  如果需要重复监听，则设置watch为true
                            LOGGER.info("reget children : {}", zooKeeper.getChildren(watchedEvent.getPath(), true));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
            connectionCountDownLantch.await();
            return zooKeeper;
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage(), e);
        }
        return null;
    }


    static class Config {
        public static final String CONNECT_STRING = "47.98.141.146:2181";
        public static final int SESSION_TIMEOUT = 5000;

    }

}
