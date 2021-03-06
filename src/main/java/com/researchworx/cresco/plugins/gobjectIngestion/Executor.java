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
                        break;
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
        } catch(Exception ex) {
            logger.error("main " + pathStageName + " : " + ex.getMessage() + " " + pme.getParams().toString());
            ex.printStackTrace();
        }
    }

    private void pathStage2 (MsgEvent pme, int pathStage) {
        String pathStageName = "pathStage" + String.valueOf(pathStage);
        try {
            MsgEvent.Type eventType = MsgEvent.Type.valueOf(pme.getParam("gmsg_type"));
            String message = pme.getParam("msg");
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
                        Plugin.objectToFSp.preprocessBaggedSequence(seqId, reqId, false);
                        break;
                    case 3:
                        break;
                    case 4:
                        break;
                    case 999:
                        Plugin.objectToFSp.endProcessSequence(seqId, reqId);
                        break;
                    default:
                        logger.error("Undefined sStep " + pathStageName + " !");
                        break;
                }
            } else if(eventType.equals(MsgEvent.Type.ERROR)) {

            } else {
                logger.error("Unknown MsgEvent.Type : " + pathStageName);
            }
        } catch(Exception ex) {
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
        } catch(Exception ex) {
            logger.error("main " + pathStageName + " : " + ex.getMessage() + " " + pme.getParams().toString());
            ex.printStackTrace();
        }
    }

    private void pathStage4 (MsgEvent pme, int pathStage) {
        String pathStageName = "pathStage" + String.valueOf(pathStage);
        try {
            MsgEvent.Type eventType = MsgEvent.Type.valueOf(pme.getParam("gmsg_type"));
            String message = pme.getParam("msg");
            String seqId = pme.getParam("seq_id");
            String reqId = pme.getParam("req_id");
            String sampleId = pme.getParam("sample_id");
            if (reqId == null)
                reqId = "unknown";
            logger.info(pathStageName + " " + message + " " + eventType.name() + " message");
            if(eventType.equals(MsgEvent.Type.INFO)) {
                int sStep = Integer.parseInt(pme.getParam("sstep"));
                switch (sStep) {
                    case 1:
                        break;
                    case 2:
                        Plugin.objectToFSp.processBaggedSample(seqId, sampleId, reqId, false);
                        break;
                    case 3:
                        Plugin.objectToFSp.processBaggedSample(seqId, sampleId, reqId, false);
                        break;
                    case 4:
                        Plugin.objectToFSp.processBaggedSample(seqId, sampleId, reqId, false);
                        break;
                    case 7:
                        Plugin.objectToFSp.processBaggedSample(seqId, sampleId, reqId, false);
                        break;
                    case 999:
                        Plugin.objectToFSp.endProcessSample(seqId, sampleId, reqId);
                        break;
                    default:
                        logger.error("Undefined sStep " + pathStageName + " !");
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
            String seqId = pme.getParam("seq_id");
            String reqId = pme.getParam("req_id");
            logger.info(pathStageName + " " + eventType.name() + " message");
            if(eventType.equals(MsgEvent.Type.INFO)) {
                int sStep = Integer.parseInt(pme.getParam("sstep"));
                switch (sStep) {
                    case 1:
                        break;
                    case 2:
                        Plugin.objectToFSp.downloadBaggedResults(seqId, reqId);
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
