package com.researchworx.cresco.plugins.gobjectIngestion;

import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.plugin.core.CExecutor;
import com.researchworx.cresco.library.utilities.CLogger;

public class Executor extends CExecutor {
    private Plugin mainPlugin;
    private CLogger logger;
    public Executor(Plugin plugin) {
        super(plugin);
        logger = new CLogger(Executor.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Trace);
    }

    @Override
    public MsgEvent processExec(MsgEvent msg) {
        logger.debug("Processing [EXEC] message");
        logger.trace("Params: " + msg.getParams());
        if(msg.getParam("pathstage") != null) { //pathstage message
            int pathStage;
            try{
                pathStage = Integer.parseInt(msg.getParam("pathstage"));
                switch (pathStage) {
                    case 1:
                        pathStage1(msg,pathStage);
                        break;
                    case 2:
                        pathStage2(msg,pathStage);
                        break;
                    case 3:
                        pathStage3(msg,pathStage);
                        break;
                    case 4:
                        pathStage4(msg,pathStage);
                        break;
                    case 5:
                        pathStage5(msg,pathStage);
                    default:
                        logger.error("Undefined pathStage!");
                        break;
                }

            } catch (Exception ex) {
                logger.error("Problem setting pathStage : " + ex.getMessage());
            }

        }
        return null;
    }

    private void pathStage1 (MsgEvent pme, int pathStage) {
        String pathStageName = "pathStage" + String.valueOf(pathStage);
        try {
            MsgEvent.Type eventType = MsgEvent.Type.valueOf(pme.getParam("gmsg_type"));
            logger.info(pathStageName + " " + eventType.name() + " message");
            if(eventType.equals(MsgEvent.Type.INFO)) {
                if(pme.getParam("pstep") != null ) {
                    int pStep = Integer.parseInt(pme.getParam("pstep"));
                    switch (pStep) {
                        case 1:
                            break;
                        case 2:
                            break;
                        case 3:
                            break;
                        case 4:
                            break;
                        default:
                            logger.error("Undefined pStep " + pathStageName + " !");
                            break;
                    }
                } else if(pme.getParam("sstep") != null ) {
                    int sStep = Integer.parseInt(pme.getParam("sstep"));
                    switch (sStep) {
                        case 1:
                            break;
                        case 2:
                            break;
                        case 3:
                            break;
                        case 4:
                            break;
                        default:
                            logger.error("Undefined pStep " + pathStageName + " !");
                            break;
                    }
                }
            } else if(eventType.equals(MsgEvent.Type.ERROR)) {

            } else {
                logger.error("Unknown MsgEvent.Type : " + pathStageName);
            }
        }
        catch(Exception ex) {
            logger.error("main " + pathStageName + " : " + ex.getMessage() + " " + pme.getParams().toString());
            ex.printStackTrace();
        }
    }

    private void pathStage2 (MsgEvent pme, int pathStage) {
        String pathStageName = "pathStage" + String.valueOf(pathStage);
        try {
            MsgEvent.Type eventType = MsgEvent.Type.valueOf(pme.getParam("gmsg_type"));

            String transfer_status_file = pme.getParam("transfer_status_file");
            String bucket_name = pme.getParam("bucket_name");
            String transfer_watch_file = pme.getParam("transfer_watch_file");
            String message = pme.getParam("msg");
            String endpoint = pme.getParam("endpoint");
            String seqId = pme.getParam("seq_id");
            String reqId = pme.getParam("req_id");
            if (reqId == null)
                reqId = "unknown";
            logger.info(pathStageName + " " + eventType.name() + " " + message + " " + pme.getParam("src_region") + "_" + pme.getParam("src_agent"));


            if(eventType.equals(MsgEvent.Type.INFO)) {
                int sStep = Integer.parseInt(pme.getParam("sstep"));
                switch (sStep) {
                    case 1:
                        break;
                    case 2:
                            Plugin.objectToFSp.processSequence(seqId, reqId, true);
                        break;
                    case 3:
                        break;
                    case 4:
                        break;
                    default:
                        logger.error("Undefined pStep " + pathStageName + " !");
                        break;
                }
            } else if(eventType.equals(MsgEvent.Type.ERROR)) {

            } else {
                logger.error("Unknown MsgEvent.Type : " + pathStageName);
            }
        }
        catch(Exception ex) {
            logger.error("main " + pathStageName + " : " + ex.getMessage() + " " + pme.getParams().toString());
            ex.printStackTrace();
        }
    }

    private void pathStage3 (MsgEvent pme, int pathStage) {
        String pathStageName = "pathStage" + String.valueOf(pathStage);
        try {
            MsgEvent.Type eventType = MsgEvent.Type.valueOf(pme.getParam("gmsg_type"));
            logger.info(pathStageName + " " + eventType.name() + " message");
            if(eventType.equals(MsgEvent.Type.INFO)) {
                int pStep = Integer.parseInt(pme.getParam("pstep"));
                switch (pStep) {
                    case 1:
                        break;
                    case 2:
                        break;
                    case 3:
                        break;
                    case 4:
                        break;
                    default:
                        logger.error("Undefined pStep " + pathStageName + " !");
                        break;
                }
            } else if(eventType.equals(MsgEvent.Type.ERROR)) {

            } else {
                logger.error("Unknown MsgEvent.Type : " + pathStageName);
            }
        }
        catch(Exception ex) {
            logger.error("main " + pathStageName + " : " + ex.getMessage() + " " + pme.getParams().toString());
            ex.printStackTrace();
        }
    }

    private void pathStage4 (MsgEvent pme, int pathStage) {
        String pathStageName = "pathStage" + String.valueOf(pathStage);
        try {
            MsgEvent.Type eventType = MsgEvent.Type.valueOf(pme.getParam("gmsg_type"));
            logger.info(pathStageName + " " + eventType.name() + " message");
            if(eventType.equals(MsgEvent.Type.INFO)) {
                int sStep = Integer.parseInt(pme.getParam("sstep"));
                switch (sStep) {
                    case 1:
                        break;
                    case 2:
                        Plugin.objectToFSp.processSample(pme.getParam("seq_id"), pme.getParam("sample_id"), pme.getParam("req_id"), true);
                        break;
                    case 3:
                        break;
                    case 4:
                        break;
                    case 7:
                        Plugin.objectToFSp.processSample(pme.getParam("seq_id"), pme.getParam("sample_id"), pme.getParam("req_id"), true);
                        break;
                    default:
                        logger.error("Undefined pStep " + pathStageName + " !");
                        break;
                }
            } else if(eventType.equals(MsgEvent.Type.ERROR)) {

            } else {
                logger.error("Unknown MsgEvent.Type : " + pathStageName);
            }
        }
        catch(Exception ex) {
            logger.error("main " + pathStageName + " : " + ex.getMessage() + " " + pme.getParams().toString());
            ex.printStackTrace();
        }
    }

    private void pathStage5 (MsgEvent pme, int pathStage) {
        String pathStageName = "pathStage" + String.valueOf(pathStage);
        try {
            MsgEvent.Type eventType = MsgEvent.Type.valueOf(pme.getParam("gmsg_type"));
            logger.info(pathStageName + " " + eventType.name() + " message");
            if(eventType.equals(MsgEvent.Type.INFO)) {
                int sStep = Integer.parseInt(pme.getParam("sstep"));
                switch (sStep) {
                    case 1:
                        break;
                    case 2:
                        Plugin.objectToFSp.downloadResults(pme.getParam("seq_id"), pme.getParam("req_id"));
                        break;
                    case 3:
                        break;
                    case 4:
                        break;
                    default:
                        logger.error("Undefined pStep " + pathStageName + " !");
                        break;
                }
            } else if(eventType.equals(MsgEvent.Type.ERROR)) {

            } else {
                logger.error("Unknown MsgEvent.Type : " + pathStageName);
            }
        }
        catch(Exception ex) {
            logger.error("main " + pathStageName + " : " + ex.getMessage() + " " + pme.getParams().toString());
            ex.printStackTrace();
        }
    }

    private void pathStageN (MsgEvent pme, int pathStage) {
        String pathStageName = "pathStage" + String.valueOf(pathStage);
        try {
            MsgEvent.Type eventType = MsgEvent.Type.valueOf(pme.getParam("gmsg_type"));
            logger.info(pathStageName + " " + eventType.name() + " message");
            if(eventType.equals(MsgEvent.Type.INFO)) {
                int pStep = Integer.parseInt(pme.getParam("pstep"));
                switch (pStep) {
                    case 1:
                        break;
                    case 2:
                        break;
                    case 3:
                        break;
                    case 4:
                        break;
                    default:
                        logger.error("Undefined pStep " + pathStageName + " !");
                        break;
                }
            } else if(eventType.equals(MsgEvent.Type.ERROR)) {

            } else {
                logger.error("Unknown MsgEvent.Type : " + pathStageName);
            }
        }
        catch(Exception ex) {
            logger.error("main " + pathStageName + " : " + ex.getMessage() + " " + pme.getParams().toString());
            ex.printStackTrace();

        }
    }

}
