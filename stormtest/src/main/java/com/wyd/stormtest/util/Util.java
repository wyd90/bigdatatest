package com.wyd.stormtest.util;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class Util {

    /**
     * 返回主机名
     * @return
     */
    public static String getHostName(){
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 返回进程ID
     * @return
     */
    public static String getPID(){
        String info = ManagementFactory.getRuntimeMXBean().getName();
        return info.split("@")[0];
    }

    /**
     * 返回线程ID
     * @return
     */
    public static String getTID(){
        return Thread.currentThread().getName();
    }

    /**
     * 返回对象信息
     * @param obj
     * @return
     */
    public static String OID(Object obj){
        String simpleName = obj.getClass().getSimpleName();
        int hash = obj.hashCode();
        return simpleName + "@" + hash;
    }

    public static void info(Object obj, String msg){
        try {
            Socket socket = new Socket("node1",8888);
            OutputStream out = socket.getOutputStream();
            out.write((getHostName() + " , " + getPID() + " , " + getTID() + " , " + OID(obj) + " , " + msg + "\n").getBytes());
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
