package com.belo.rocketmq.replicator.worker;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.Message;
import org.apache.helix.participant.StateMachineEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by liqiang on 2017/9/27.
 */
public class WorkerStarter {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerStarter.class);

    private String helixClusterName;
    private String instanceId;
    private String zkServer;
    private HelixManager helixManager;
    private MQProducer mqProducer;
    private WorkerConf workerConf;
    private RocketmqConnector connector;

    public WorkerStarter(WorkerConf workerConf) {
        this.workerConf = workerConf;
    }

    public static void main(String[] args) {
        WorkerConf workerConf = null;
        try {
            workerConf = WorkerConf.getWorkerConf();
        } catch (Exception e) {
            LOGGER.error("Conf get error, exit", e);
            System.exit(0);
        }

        if(!workerConf.checkWorkerConf()){
            //参数检查不通过，直接退出
            LOGGER.error("Conf check error,exit");
            System.exit(0);
        }

        final WorkerStarter workerStarter = new WorkerStarter(workerConf);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    workerStarter.stop();
                } catch (Exception e) {
                    LOGGER.error("Caught error during shutdown! ", e);
                }
            }
        });

        try {
            workerStarter.start();
        } catch (Exception e) {
            LOGGER.error("Cannot start worker: ", e);
        }
        
    }

    private void start() throws Exception {
        try {
            this.mqProducer = new MQProducer(this.workerConf);

            this.zkServer = this.workerConf.getZkStr();
            this.instanceId = this.workerConf.getWorkerInstanceId() + System.currentTimeMillis();
            this.helixClusterName = this.workerConf.getHelixClusterName();

            this.connector = new RocketmqConnector(this.workerConf, this.mqProducer);

            addToHelixController();
        }catch (Exception e){
            LOGGER.error("WorkerStarter start error:{}", e);
            throw e;
        }
    }

    private void addToHelixController() throws Exception {
        this.helixManager = HelixManagerFactory.getZKHelixManager(this.helixClusterName, this.instanceId, InstanceType.PARTICIPANT, this.zkServer);
        StateMachineEngine stateMachineEngine = this.helixManager.getStateMachineEngine();
        HelixWorkerOnlineOfflineStateModelFactory stateModelFactory = new HelixWorkerOnlineOfflineStateModelFactory(this.instanceId, this.connector);
        stateMachineEngine.registerStateModelFactory(OnlineOfflineStateModel.name, stateModelFactory);
        this.helixManager.connect();
        this.helixManager.getMessagingService().registerMessageHandlerFactory(
                Message.MessageType.STATE_TRANSITION.name(), stateMachineEngine);
    }

    private void stop() throws Exception {
        try {

            LOGGER.info("Disconnect with helixZkManager");
            this.helixManager.disconnect();

            LOGGER.info("Shutting down connectors.");
            this.connector.shutdown();

            LOGGER.info("Closing producer.");
            this.mqProducer.stop();

            LOGGER.info("WorkerStarter shutdown successfully");
        }catch (Exception e){
            LOGGER.error("WorkerStarter stop error:{}", e);
            throw e;
        }

    }
}
