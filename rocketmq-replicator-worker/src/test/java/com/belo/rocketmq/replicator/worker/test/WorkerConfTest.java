package com.belo.rocketmq.replicator.worker.test;

import com.belo.rocketmq.replicator.worker.WorkerConf;
import org.junit.Test;

/**
 * Created by liqiang on 2017/10/19.
 */
public class WorkerConfTest {

    @Test
    public void testWorkerConfFromTaihe(){
        try {
            WorkerConf workerConf = WorkerConf.getWorkerConf();
            System.out.println(workerConf);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

}
