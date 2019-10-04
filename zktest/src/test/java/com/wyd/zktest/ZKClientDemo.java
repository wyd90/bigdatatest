package com.wyd.zktest;

import org.apache.zookeeper.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class ZKClientDemo {

    private ZooKeeper zooKeeper;

    private ZooKeeper zk;

    @Before
    public void init() throws IOException {
        //构建一个连接zookeeper的客户端
        zooKeeper = new ZooKeeper("node2:2181,node3:2181,node4:2181", 2000, null);
    }

    @Test
    public void testCreate() throws IOException, KeeperException, InterruptedException {



        //返回创建的路径                                                                  开放权限                          节点类型
        String create = zooKeeper.create("/eclipse", "hello eclipse".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        System.out.println(create);

    }

    @Test
    public void testUpdate() throws KeeperException, InterruptedException {
        //                                                         -1就是立即修改所有版本
        zooKeeper.setData("/eclipse","hello word".getBytes(),-1);
    }

    @Test
    public void testGet() throws KeeperException, InterruptedException, UnsupportedEncodingException {
        byte[] data = zooKeeper.getData("/eclipse", false, null);
        System.out.println(new String(data, "UTF-8"));
    }

    @Test
    public void testListChildren() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren("/eclipse", false);
        for(String node : children){
            System.out.println(node);
        }
    }

    public void testRM() throws KeeperException, InterruptedException {
        //                                  删除所有版本
        zooKeeper.delete("/eclipse", -1);
    }

    @Test
    public void testGetWatch() throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData("/mygirls", new Watcher() {

            @Override
            public void process(WatchedEvent event) {
                System.out.println(event.getPath()); //收到的事件所发生的节点路径
                System.out.println(event.getType()); //收到事件的类型

            }
        }, null);
        //   一些节点参数

    }

    @Test
    public void testWatchZk() throws IOException, KeeperException, InterruptedException {

        zk = new ZooKeeper("node2:2181,node3:2181,node4:2181", 2000, new Watcher() {

            @Override
            public void process(WatchedEvent event) {

                try {
                    if(event.getState() == Event.KeeperState.SyncConnected && event.getType() == Event.EventType.NodeDataChanged){

                        byte[] data = zk.getData("", true, null);

                    } else if(event.getState() == Event.KeeperState.SyncConnected && event.getType() == Event.EventType.NodeChildrenChanged) {
                        List<String> children = zk.getChildren("/", true);
                    }
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        zk.getChildren("/",true);
        zk.getData("/", true, null);
    }

    @After
    public void cleanUp() throws InterruptedException {
        zooKeeper.close();
    }
}

