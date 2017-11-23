package com.belo.rocketmq.replicator.controller.backup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * 文件BackUp处理,备份的数据保存在本机
 * Created by liqiang on 2017/9/27.
 */
public class FileBackUpHandler extends BackUpHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileBackUpHandler.class);

    private String localPath = "";

    public FileBackUpHandler(String localPath) {
        this.localPath = localPath;
    }

    public void writeToFile(String fileName, String data) throws Exception {
        BufferedWriter output = null;
        try {
            File myfile = new File(localPath + "/" + fileName);

            try {
                output = new BufferedWriter(new FileWriter(myfile));
                output.write(data);
                output.flush();
                LOGGER.info("Successful backup of file " + fileName);
            } catch (IOException e) {
                LOGGER.error("Error writing backup to the file " + fileName);
                throw e;
            }

        } catch (Exception e) {
            throw e;
        } finally {
            output.close();
        }
    }

}
