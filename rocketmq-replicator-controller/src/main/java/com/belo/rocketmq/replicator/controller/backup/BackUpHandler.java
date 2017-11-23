package com.belo.rocketmq.replicator.controller.backup;

/**
 * backup的抽象类
 * Created by liqiang on 2017/9/27.
 */
public abstract class BackUpHandler {

    public abstract void writeToFile(String fileName, String data) throws Exception;

}
