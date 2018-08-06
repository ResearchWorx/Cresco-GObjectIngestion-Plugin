package com.researchworx.cresco.plugins.gobjectIngestion.folderprocessor;

import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import com.researchworx.cresco.plugins.gobjectIngestion.Plugin;
import com.researchworx.cresco.plugins.gobjectIngestion.objectstorage.Encapsulation;
import com.researchworx.cresco.plugins.gobjectIngestion.objectstorage.ObjectEngine;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class ObjectFS implements Runnable {

    private final String transfer_watch_file;
    private final String transfer_status_file;

    private String bucket_name;
    private String incoming_directory;
    private String outgoing_directory;
    private Plugin plugin;
    private CLogger logger;
    private String pathStage;
    private int pstep;
    private String stagePhase;

    public ObjectFS(Plugin plugin) {
        this.stagePhase = "uninit";
        this.pstep = 1;
        this.plugin = plugin;
        this.logger = new CLogger(ObjectFS.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Trace);
        this.pathStage = String.valueOf(plugin.pathStage);
        logger.debug("OutPathPreProcessor Instantiated");
        incoming_directory = plugin.getConfig().getStringParam("incoming_directory");
        logger.debug("\"pathstage" + pathStage + "\" --> \"incoming_directory\" from config [{}]", incoming_directory);
        outgoing_directory = plugin.getConfig().getStringParam("outgoing_directory");
        logger.debug("\"pathstage" + pathStage + "\" --> \"outgoing_directory\" from config [{}]", outgoing_directory);
        transfer_status_file = plugin.getConfig().getStringParam("transfer_status_file");
        logger.debug("\"pathstage" + pathStage + "\" --> \"transfer_status_file\" from config [{}]", transfer_status_file);

        transfer_watch_file = plugin.getConfig().getStringParam("transfer_watch_file");
        logger.debug("\"pathstage" + pathStage + "\" --> \"transfer_watch_file\" from config [{}]", transfer_watch_file);

        bucket_name = plugin.getConfig().getStringParam("bucket");
        logger.debug("\"pathstage" + pathStage + "\" --> \"bucket\" from config [{}]", bucket_name);

        MsgEvent me = plugin.genGMessage(MsgEvent.Type.INFO, "InPathPreProcessor instantiated");
        //me.setParam("transfer_watch_file", transfer_watch_file);
        //me.setParam("transfer_status_file", transfer_status_file);
        //me.setParam("bucket_name", bucket_name);
        me.setParam("pathstage", pathStage);
        //me.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
        me.setParam("pstep", String.valueOf(pstep));
        plugin.sendMsgEvent(me);
    }

    @Override
    public void run() {
        try {
            pstep = 2;
            logger.trace("Setting [PathProcessorActive] to true");
            plugin.PathProcessorActive = true;
            logger.trace("Entering while-loop");
            while (plugin.PathProcessorActive) {
                sendUpdateInfoMessage(null, null, null, String.valueOf(pstep), "Idle");
                Thread.sleep(plugin.getConfig().getIntegerParam("scan_interval", 5000));
            }
        } catch (Exception e) {
            sendUpdateErrorMessage(null, null, null, String.valueOf(pstep),
                    String.format("General Run Exception [%s:%s]", e.getClass().getCanonicalName(), e.getMessage()));
        }
    }

    public void processBaggedSequence(String seqId, String reqId, boolean trackPerf) {
        logger.debug("processBaggedSequence('{}','{}',{})", seqId, reqId, trackPerf);
        ObjectEngine oe = new ObjectEngine(plugin);
        pstep = 3;
        int sstep = 0;
        String clinical_bucket_name = plugin.getConfig().getStringParam("clinical_bucket");
        logger.trace("Checking to see if clinical bucket [{}] exists", clinical_bucket_name);
        if (clinical_bucket_name == null || clinical_bucket_name.equals("") ||
                !oe.doesBucketExist(clinical_bucket_name)) {
            sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                    String.format("Clinical bucket [%s] does not exist",
                            clinical_bucket_name != null ? clinical_bucket_name : "NULL"));
            plugin.PathProcessorActive = false;
            return;
        }
        String research_bucket_name = plugin.getConfig().getStringParam("research_bucket");
        logger.trace("Checking to see if research bucket [{}] exists", research_bucket_name);
        if (research_bucket_name == null || research_bucket_name.equals("") ||
                !oe.doesBucketExist(research_bucket_name)) {
            sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                    String.format("Research bucket [%s] does not exist",
                            research_bucket_name != null ? research_bucket_name : "NULL"));
            plugin.PathProcessorActive = false;
            return;
        }
        sstep = 1;
        String remoteDir = seqId;
        MsgEvent pse;
        String workDirName = null;
        try {
            workDirName = incoming_directory;
            workDirName = workDirName.replace("//", "/");
            if (!workDirName.endsWith("/")) {
                workDirName += "/";
            }
            File workDir = new File(workDirName);
            if (workDir.exists()) {
                if (!deleteDirectory(workDir))
                    logger.error("deleteDirectory('{}') = false", workDir.getAbsolutePath());
            }
            if (!workDir.mkdir())
                logger.error("workDir.mkdir() = false (workDir = '{}')", workDir.getAbsolutePath());
            sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                    "Retrieving bagged sequence");
            sstep = 2;
            if (oe.downloadBaggedDirectory(bucket_name, remoteDir, workDirName, seqId, null, reqId,
                    String.valueOf(sstep))) {
                String baggedSequenceFile = workDirName + seqId + ".tar";
                sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                        String.format("Download successful, unboxing sequence file [%s]", baggedSequenceFile));
                /*if (!Encapsulation.unBoxIt(baggedSequenceFile)) {
                    sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                            String.format("Failed to unbox sequence file [%s]", baggedSequenceFile));
                    pstep = 2;
                    return;
                }*/
                Encapsulation.decompress(baggedSequenceFile, workDir);
                String unboxed = workDirName + seqId + "/";
                if (!new File(unboxed).exists() || !new File(unboxed).isDirectory()) {
                    sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                            String.format("Unboxing to [%s] failed", unboxed));
                    pstep = 2;
                    return;
                }
                logger.trace("unBoxIt result: {}, deleting TAR file", unboxed);
                new File(baggedSequenceFile).delete();
                sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                        String.format("Validating sequence [%s]", unboxed));
                if (!Encapsulation.isBag(unboxed)) {
                    sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                            String.format("Unboxed sequence [%s] does not contain BagIt data", unboxed));
                    pstep = 2;
                    return;
                }
                if (!Encapsulation.verifyBag(unboxed, true)) {
                    sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                            String.format("Unboxed sequence [%s] failed BagIt verification", unboxed));
                    pstep = 2;
                    return;
                }
                sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                        String.format("Restoring sequence [%s]", unboxed));
                Encapsulation.debagify(unboxed);
                //workDirName = unboxed;
                sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                        String.format("Sequence [%s] restored to [%s]", seqId, unboxed));
                sstep = 3;
            }
        } catch (Exception e) {
            sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                    String.format("Exception encountered: [%s:%s]", e.getClass().getCanonicalName(), e.getMessage()));
        }
        if (sstep == 3) {
            try {
                //start perf mon
                PerfTracker pt = null;
                if (trackPerf) {
                    logger.trace("Starting performance monitoring");
                    pt = new PerfTracker();
                    new Thread(pt).start();
                }
                sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                        "Creating output directory");
                String resultDirName = outgoing_directory; //create random tmp location
                logger.trace("Clearing/resetting output directory");
                resultDirName = resultDirName.replace("//", "/");
                if (!resultDirName.endsWith("/")) {
                    resultDirName += "/";
                }
                File resultDir = new File(resultDirName);
                if (resultDir.exists()) {
                    deleteDirectory(resultDir);
                }
                logger.trace("Creating output directory: {}", resultDirName);
                resultDir.mkdir();

                String clinicalResultsDirName = resultDirName + "clinical/";
                if (new File(clinicalResultsDirName).exists())
                    deleteDirectory(new File(clinicalResultsDirName));
                new File(clinicalResultsDirName).mkdir();
                String researchResultsDirName = resultDirName + "research/";
                if (new File(researchResultsDirName).exists())
                    deleteDirectory(new File(researchResultsDirName));
                new File(researchResultsDirName).mkdir();
                sstep = 4;
                sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                        "Starting Pre-Processing via Docker Container");
                String containerName = "procseq." + seqId;
                Process kill = Runtime.getRuntime().exec("docker kill " + containerName);
                kill.waitFor();
                Process clear = Runtime.getRuntime().exec("docker rm " + containerName);
                clear.waitFor();
                String command = "docker run " +
                        "-v " + researchResultsDirName + ":/gdata/output/research " +
                        "-v " + clinicalResultsDirName + ":/gdata/output/clinical " +
                        "-v " + workDirName + ":/gdata/input " +
                        "-e INPUT_FOLDER_PATH=/gdata/input/" + remoteDir + " " +
                        "--name " + containerName + " " +
                        "-t 850408476861.dkr.ecr.us-east-1.amazonaws.com/gbase /opt/pretools/raw_data_processing.pl";
                logger.trace("Running Docker Command: {}", command);
                StringBuilder output = new StringBuilder();
                Process p = null;
                try {
                    p = Runtime.getRuntime().exec(command);
                    logger.trace("Attaching output reader");
                    BufferedReader outputFeed = new BufferedReader(new InputStreamReader(p.getInputStream()));
                    String outputLine;
                    while ((outputLine = outputFeed.readLine()) != null) {
                        output.append(outputLine);
                        output.append("\n");
                        String[] outputStr = outputLine.split("\\|\\|");
                        for (int i = 0; i < outputStr.length; i++)
                            outputStr[i] = outputStr[i].trim();
                        if ((outputStr.length == 5) && ((outputLine.toLowerCase().startsWith("info")) ||
                                (outputLine.toLowerCase().startsWith("error")))) {
                            if (outputStr[0].toLowerCase().equals("info")) {
                                if (!stagePhase.equals(outputStr[3])) {
                                    sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                                            String.format("Pipeline now in phase %s", outputStr[3]));
                                }
                                stagePhase = outputStr[3];
                            } else if (outputStr[0].toLowerCase().equals("error")) {
                                sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                                        String.format("Pre-Processing error: %s", outputLine));
                            }
                        }
                        logger.debug(outputLine);
                    }
                    logger.trace("Waiting for Docker process to finish");
                    p.waitFor();
                    logger.trace("Docker Exit Code: {}", p.exitValue());
                    if (trackPerf) {
                        logger.trace("Ending Performance Monitor");
                        pt.isActive = false;
                        logger.trace("Sending Performance Information");
                        pse = plugin.genGMessage(MsgEvent.Type.INFO, "Sending Performance Information");
                        pse.setParam("req_id", reqId);
                        pse.setParam("seq_id", seqId);
                        pse.setParam("pathstage", pathStage);
                        pse.setParam("sstep", String.valueOf(sstep));
                        pse.setParam("perf_log", pt.getResults());
                        plugin.sendMsgEvent(pse);
                    }
                    pse = plugin.genGMessage(MsgEvent.Type.INFO, "Sending Pipeline Output");
                    pse.setParam("req_id", reqId);
                    pse.setParam("seq_id", seqId);
                    pse.setParam("pathstage", pathStage);
                    pse.setParam("sstep", String.valueOf(sstep));
                    pse.setParam("output_log", output.toString());
                    plugin.sendMsgEvent(pse);
                } catch (IOException ioe) {
                    sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                            String.format("File read/write exception [%s:%s]",
                                    ioe.getClass().getCanonicalName(), ioe.getMessage()));
                } catch (InterruptedException ie) {
                    sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                            String.format("Process was interrupted [%s:%s]",
                                    ie.getClass().getCanonicalName(), ie.getMessage()));
                } catch (Exception e) {
                    sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                            String.format("Exception [%s:%s]",
                                    e.getClass().getCanonicalName(), e.getMessage()));
                }
                logger.trace("Pipeline has finished");
                Thread.sleep(2000);
                if (p != null) {
                    switch (p.exitValue()) {
                        case 0:     // Container finished successfully
                            sstep = 5;
                            sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                                    "Pipeline has completed");
                            sstep = 6;
                            /*if (new File(resultDirName + "clinical/" + seqId + "/").exists()) {
                                sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                                        "Transferring Clinical Results Directory");
                                if (oe.uploadBaggedDirectory(clinical_bucket_name, resultDirName + "clinical/" +
                                        seqId + "/", seqId, seqId, null, reqId, String.valueOf(sstep))) {
                                    sstep = 7;
                                    logger.debug("Results Directory Sycned [inDir = {}]", resultDir);
                                    logger.trace("Sample Directory: " + resultDirName + "clinical/" +
                                            seqId + "/");
                                    String sampleList = getSampleList(resultDirName + "clinical/" + seqId + "/");
                                    pse = plugin.genGMessage(MsgEvent.Type.INFO,
                                            "Clinical Results Directory Transfer Complete");
                                    pse.setParam("req_id", reqId);
                                    pse.setParam("seq_id", seqId);
                                    pse.setParam("pathstage", pathStage);
                                    if (sampleList != null) {
                                        logger.trace("Samples : " + sampleList);
                                        pse.setParam("sample_list", sampleList);
                                    } else {
                                        pse.setParam("sample_list", "");
                                    }
                                    pse.setParam("sstep", String.valueOf(sstep));
                                    plugin.sendMsgEvent(pse);
                                }
                            }
                            if (new File(resultDirName + "research/" + seqId + "/").exists()) {
                                sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                                        "Transferring Research Results Directory");
                                if (oe.uploadBaggedDirectory(research_bucket_name, resultDirName + "research/" +
                                        seqId + "/", seqId, seqId, null, reqId, String.valueOf(sstep))) {
                                    sstep = 8;
                                    logger.debug("Results Directory Sycned [inDir = {}]", resultDir);
                                    pse = plugin.genGMessage(MsgEvent.Type.INFO,
                                            "Research Results Directory Transferred");
                                    pse.setParam("req_id", reqId);
                                    pse.setParam("seq_id", seqId);
                                    pse.setParam("pathstage", pathStage);
                                    pse.setParam("sstep", String.valueOf(sstep));
                                    plugin.sendMsgEvent(pse);
                                }
                            }*/
                            sstep = 9;
                            sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                                    "Pre-processing is complete");
                            break;
                        case 1:     // Container error encountered
                            sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                                    "General Docker error encountered (Err: 1)");
                            break;
                        case 100:   // Script failure encountered
                            sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                                    "Pre-Processor encountered an error (Err: 100)");
                            break;
                        case 125:   // Container failed to run
                            sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                                    "Container failed to run (Err: 125)");
                            break;
                        case 126:   // Container command cannot be invoked
                            sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                                    "Container command failed to be invoked (Err: 126)");
                            break;
                        case 127:   // Container command cannot be found
                            sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                                    "Container command could not be found (Err: 127)");
                            break;
                        case 137:   // Container was killed
                            sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                                    "Container was manually stopped (Err: 137)");
                            break;
                        default:    // Other return code encountered
                            sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                                    String.format("Unknown container return code (Err: %d)", p.exitValue()));
                            break;
                    }
                } else {
                    sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                            "Error retrieving return code from container");
                }
                Process postClear = Runtime.getRuntime().exec("docker rm " + containerName);
                postClear.waitFor();
            } catch (Exception e) {
                sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                        String.format("processBaggedSequence exception encountered [%s:%s]",
                                e.getClass().getCanonicalName(), e.getMessage()));
            }
        }
        pstep = 2;
    }

    public void processSequence(String seqId, String reqId, boolean trackPerf) {
        logger.debug("Call to processSequence seq_id: " + seqId, ", req_id: " + reqId);

        pstep = 3;
        int sstep = 0;
        String clinical_bucket_name = plugin.getConfig().getStringParam("clinical_bucket");
        if (clinical_bucket_name == null || clinical_bucket_name.equals("")) {
            MsgEvent error = plugin.genGMessage(MsgEvent.Type.ERROR, "Configuration value [clinical_bucket] is not properly set");
            error.setParam("req_id", reqId);
            error.setParam("seq_id", seqId);
            error.setParam("transfer_status_file", transfer_status_file);
            error.setParam("bucket_name", bucket_name);
            error.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
            error.setParam("pathstage", pathStage);
            error.setParam("sstep", String.valueOf(sstep));
            plugin.sendMsgEvent(error);
            plugin.PathProcessorActive = false;
            return;
        }
        String research_bucket_name = plugin.getConfig().getStringParam("research_bucket");
        if (research_bucket_name == null || research_bucket_name.equals("")) {
            MsgEvent error = plugin.genGMessage(MsgEvent.Type.ERROR, "Configuration value [research_bucket] is not properly set");
            error.setParam("req_id", reqId);
            error.setParam("seq_id", seqId);
            error.setParam("transfer_status_file", transfer_status_file);
            error.setParam("bucket_name", bucket_name);
            error.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
            error.setParam("pathstage", pathStage);
            error.setParam("sstep", String.valueOf(sstep));
            plugin.sendMsgEvent(error);
            plugin.PathProcessorActive = false;
            return;
        }
        sstep = 1;

        String remoteDir = seqId + "/";

        MsgEvent pse;
        String workDirName = null;
        try {
            workDirName = incoming_directory; //create random tmp location
            workDirName = workDirName.replace("//", "/");
            if (!workDirName.endsWith("/")) {
                workDirName += "/";
            }
            File workDir = new File(workDirName);
            if (workDir.exists()) {
                deleteDirectory(workDir);
            }
            workDir.mkdir();

            List<String> filterList = new ArrayList<>();
            logger.trace("Add [transfer_status_file] to [filterList]");

            ObjectEngine oe = new ObjectEngine(plugin);

            if (new File(workDirName + seqId).exists()) {
                deleteDirectory(new File(workDirName + seqId));
            }
            pse = plugin.genGMessage(MsgEvent.Type.INFO, "Directory Transfering");
            //me.setParam("inDir", remoteDir);
            //me.setParam("outDir", incoming_directory);
            pse.setParam("seq_id", seqId);
            pse.setParam("req_id", reqId);
            pse.setParam("transfer_status_file", transfer_status_file);
            pse.setParam("bucket_name", bucket_name);
            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
            pse.setParam("pathstage", pathStage);
            pse.setParam("sstep", String.valueOf(sstep));
            plugin.sendMsgEvent(pse);
            sstep = 2;

            oe.downloadDirectory(bucket_name, remoteDir, workDirName, seqId, null);

            //logger.debug("[inDir = {}]", inDir);
            oe = new ObjectEngine(plugin);
            if (oe.isSyncDir(bucket_name, remoteDir, workDirName + seqId, filterList)) {
                logger.debug("Directory Sycned [inDir = {}]", workDirName);
                //Map<String, String> md5map = oe.getDirMD5(inDir, filterList);
                //logger.trace("Set MD5 hash");
                //setTransferFileMD5(inDir + transfer_status_file, md5map);
                pse = plugin.genGMessage(MsgEvent.Type.INFO, "Directory Transfered");
                pse.setParam("indir", workDirName);
                pse.setParam("seq_id", seqId);
                pse.setParam("req_id", reqId);
                pse.setParam("transfer_status_file", transfer_status_file);
                pse.setParam("bucket_name", bucket_name);
                pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                pse.setParam("pathstage", pathStage);
                pse.setParam("sstep", String.valueOf(sstep));
                plugin.sendMsgEvent(pse);
                sstep = 3;
            }
        } catch (Exception ex) {
            logger.error("run {}", ex.getMessage());
            pse = plugin.genGMessage(MsgEvent.Type.ERROR, "Error Path Run");
            pse.setParam("seq_id", seqId);
            pse.setParam("req_id", reqId);
            pse.setParam("transfer_watch_file", transfer_watch_file);
            pse.setParam("transfer_status_file", transfer_status_file);
            pse.setParam("bucket_name", bucket_name);
            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
            pse.setParam("pathstage", pathStage);
            pse.setParam("error_message", ex.getMessage());
            pse.setParam("sstep", String.valueOf(sstep));
            plugin.sendMsgEvent(pse);
        }

        //if is makes it through process the seq
        if (sstep == 3) {
            try {
                //start perf mon
                PerfTracker pt = null;
                if (trackPerf) {
                    logger.trace("Starting performance monitoring");
                    pt = new PerfTracker();
                    new Thread(pt).start();
                }

                pse = plugin.genGMessage(MsgEvent.Type.INFO, "Creating output directory");
                pse.setParam("indir", workDirName);
                pse.setParam("req_id", reqId);
                pse.setParam("seq_id", seqId);
                pse.setParam("transfer_status_file", transfer_status_file);
                pse.setParam("bucket_name", bucket_name);
                pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                pse.setParam("pathstage", pathStage);
                pse.setParam("sstep", String.valueOf(sstep));
                plugin.sendMsgEvent(pse);

                String resultDirName = outgoing_directory; //create random tmp location
                logger.trace("Clearing/resetting output directory");
                resultDirName = resultDirName.replace("//", "/");
                if (!resultDirName.endsWith("/")) {
                    resultDirName += "/";
                }
                File resultDir = new File(resultDirName);
                if (resultDir.exists()) {
                    deleteDirectory(resultDir);
                }
                logger.trace("Creating output directory: {}", resultDirName);
                resultDir.mkdir();

                String clinicalResultsDirName = resultDirName + "clinical/";
                if (new File(clinicalResultsDirName).exists())
                    deleteDirectory(new File(clinicalResultsDirName));
                new File(clinicalResultsDirName).mkdir();
                String researchResultsDirName = resultDirName + "research/";
                if (new File(researchResultsDirName).exists())
                    deleteDirectory(new File(researchResultsDirName));
                new File(researchResultsDirName).mkdir();

                sstep = 4;
                pse = plugin.genGMessage(MsgEvent.Type.INFO, "Starting Pre-Processor via Docker Container");
                pse.setParam("indir", workDirName);
                pse.setParam("outdir", resultDirName);
                pse.setParam("req_id", reqId);
                pse.setParam("seq_id", seqId);
                pse.setParam("transfer_status_file", transfer_status_file);
                pse.setParam("bucket_name", bucket_name);
                pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                pse.setParam("pathstage", pathStage);
                pse.setParam("sstep", String.valueOf(sstep));
                plugin.sendMsgEvent(pse);

                String containerName = "procseq." + seqId;

                Process kill = Runtime.getRuntime().exec("docker kill " + containerName);
                kill.waitFor();

                Process clear = Runtime.getRuntime().exec("docker rm " + containerName);
                clear.waitFor();

                String command = "docker run " +
                        "-v " + researchResultsDirName + ":/gdata/output/research " +
                        "-v " + clinicalResultsDirName + ":/gdata/output/clinical " +
                        "-v " + workDirName + ":/gdata/input " +
                        "-e INPUT_FOLDER_PATH=/gdata/input/" + remoteDir + " " +
                        "--name " + containerName + " " +
                        "-t intrepo.uky.edu:5000/gbase /opt/pretools/raw_data_processing.pl";

                logger.trace("Running Docker Command: {}", command);

                StringBuilder output = new StringBuilder();
                Process p = null;
                try {
                    p = Runtime.getRuntime().exec(command);
                    logger.trace("Attaching output reader");
                    BufferedReader outputFeed = new BufferedReader(new InputStreamReader(p.getInputStream()));
                    String outputLine;
                    while ((outputLine = outputFeed.readLine()) != null) {
                        output.append(outputLine);
                        output.append("\n");

                        String[] outputStr = outputLine.split("\\|\\|");

                        for (int i = 0; i < outputStr.length; i++) {
                            outputStr[i] = outputStr[i].trim();
                        }

                        if ((outputStr.length == 5) && ((outputLine.toLowerCase().startsWith("info")) || (outputLine.toLowerCase().startsWith("error")))) {
                            if (outputStr[0].toLowerCase().equals("info")) {
                                if (!stagePhase.equals(outputStr[3])) {
                                    pse = plugin.genGMessage(MsgEvent.Type.INFO, "Pipeline now in phase " + outputStr[3]);
                                    pse.setParam("indir", workDirName);
                                    pse.setParam("outdir", resultDirName);
                                    pse.setParam("req_id", reqId);
                                    pse.setParam("seq_id", seqId);
                                    pse.setParam("transfer_status_file", transfer_status_file);
                                    pse.setParam("bucket_name", bucket_name);
                                    pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                                    pse.setParam("pathstage", pathStage);
                                    pse.setParam("sstep", String.valueOf(sstep));
                                    plugin.sendMsgEvent(pse);
                                }
                                stagePhase = outputStr[3];
                            } else if (outputStr[0].toLowerCase().equals("error")) {
                                logger.error("Pre-Processing Error : " + outputLine);
                                pse = plugin.genGMessage(MsgEvent.Type.ERROR, "");
                                pse.setParam("indir", workDirName);
                                pse.setParam("outdir", resultDirName);
                                pse.setParam("req_id", reqId);
                                pse.setParam("seq_id", seqId);
                                pse.setParam("transfer_status_file", transfer_status_file);
                                pse.setParam("bucket_name", bucket_name);
                                pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                                pse.setParam("pathstage", pathStage);
                                pse.setParam("sstep", String.valueOf(sstep));
                                pse.setParam("error_message", outputLine);
                                plugin.sendMsgEvent(pse);
                            }
                        }
                        logger.debug(outputLine);
                    }
                    logger.trace("Waiting for Docker process to finish");

                    p.waitFor();
                    logger.trace("Docker Exit Code: {}", p.exitValue());

                    if (trackPerf) {
                        logger.trace("Ending Performance Monitor");
                        pt.isActive = false;
                        logger.trace("Sending Performance Information");
                        pse = plugin.genGMessage(MsgEvent.Type.INFO, "Sending Performance Information");
                        pse.setParam("indir", workDirName);
                        pse.setParam("req_id", reqId);
                        pse.setParam("seq_id", seqId);
                        pse.setParam("transfer_status_file", transfer_status_file);
                        pse.setParam("bucket_name", bucket_name);
                        pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                        pse.setParam("pathstage", pathStage);
                        pse.setParam("sstep", String.valueOf(sstep));
                        pse.setParam("perf_log", pt.getResults());
                        plugin.sendMsgEvent(pse);
                    }
                    pse = plugin.genGMessage(MsgEvent.Type.INFO, "Sending Pipeline Output");
                    pse.setParam("indir", workDirName);
                    pse.setParam("req_id", reqId);
                    pse.setParam("seq_id", seqId);
                    pse.setParam("transfer_status_file", transfer_status_file);
                    pse.setParam("bucket_name", bucket_name);
                    pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                    pse.setParam("pathstage", pathStage);
                    pse.setParam("sstep", String.valueOf(sstep));
                    pse.setParam("output_log", output.toString());
                    plugin.sendMsgEvent(pse);
                } catch (IOException ioe) {
                    // WHAT!?! DO SOMETHIN'!
                    logger.error("File read/write exception: {}", ioe.getMessage());
                } catch (InterruptedException ie) {
                    // WHAT!?! DO SOMETHIN'!
                    logger.error("Process was interrupted: {}", ie.getMessage());
                } catch (Exception e) {
                    // WHAT!?! DO SOMETHIN'!
                    logger.error("Exception: {}", e.getMessage());
                }

                logger.trace("Pipeline has finished");

                Thread.sleep(2000);

                if (p != null) {
                    switch (p.exitValue()) {
                        case 0:     // Container finished successfully
                            sstep = 5;
                            pse = plugin.genGMessage(MsgEvent.Type.INFO, "Pipeline has completed");
                            pse.setParam("indir", workDirName);
                            pse.setParam("req_id", reqId);
                            pse.setParam("seq_id", seqId);
                            pse.setParam("transfer_status_file", transfer_status_file);
                            pse.setParam("bucket_name", bucket_name);
                            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                            pse.setParam("pathstage", pathStage);
                            pse.setParam("sstep", String.valueOf(sstep));
                            plugin.sendMsgEvent(pse);

                            ObjectEngine oe = new ObjectEngine(plugin);

                            pse = plugin.genGMessage(MsgEvent.Type.INFO, "Deleting old pre-processed files from Object Store");
                            //me.setParam("inDir", remoteDir);
                            //me.setParam("outDir", incoming_directory);
                            pse.setParam("seq_id", seqId);
                            pse.setParam("req_id", reqId);
                            pse.setParam("transfer_status_file", transfer_status_file);
                            pse.setParam("bucket_name", bucket_name);
                            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                            pse.setParam("pathstage", pathStage);
                            pse.setParam("sstep", String.valueOf(sstep));
                            plugin.sendMsgEvent(pse);

                            logger.trace("Deleting old clinical files");
                            oe.deleteBucketDirectoryContents(clinical_bucket_name, seqId);
                            logger.trace("Deleting old research files");
                            oe.deleteBucketDirectoryContents(research_bucket_name, seqId);

                            logger.trace("Uploading results to objectStore");

                            sstep = 6;

                            List<String> filterList = new ArrayList<>();
                            logger.trace("Add [transfer_status_file] to [filterList]");

                            logger.trace("Transferring Clinical Results Directory");
                            pse = plugin.genGMessage(MsgEvent.Type.INFO, "Transferring Clinical Results Directory");
                            pse.setParam("indir", workDirName);
                            pse.setParam("req_id", reqId);
                            pse.setParam("seq_id", seqId);
                            pse.setParam("transfer_status_file", transfer_status_file);
                            pse.setParam("bucket_name", bucket_name);
                            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                            pse.setParam("pathstage", pathStage);
                            pse.setParam("sstep", String.valueOf(sstep));
                            plugin.sendMsgEvent(pse);

                            oe.uploadSequenceDirectory(clinical_bucket_name, resultDirName + "clinical/" + seqId + "/", seqId, seqId, String.valueOf(sstep));

                            logger.trace("Checking if Clinical Results are Synced Properly");
                            pse = plugin.genGMessage(MsgEvent.Type.INFO, "Checking if Clinical Results are Synced Properly");
                            pse.setParam("indir", workDirName);
                            pse.setParam("req_id", reqId);
                            pse.setParam("seq_id", seqId);
                            pse.setParam("transfer_status_file", transfer_status_file);
                            pse.setParam("bucket_name", bucket_name);
                            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                            pse.setParam("pathstage", pathStage);
                            pse.setParam("sstep", String.valueOf(sstep));
                            plugin.sendMsgEvent(pse);

                            sstep = 7;

                            oe = new ObjectEngine(plugin);
                            if (oe.isSyncDir(clinical_bucket_name, seqId + "/", resultDirName + "clinical/" + seqId + "/", filterList)) {
                                logger.debug("Results Directory Sycned [inDir = {}]", resultDir);
                                logger.trace("Sample Directory: " + resultDirName + "clinical/" + seqId + "/");
                                String sampleList = getSampleList(resultDirName + "clinical/" + seqId + "/");
                                //Map<String, String> md5map = oe.getDirMD5(workDirName, filterList);
                                //logger.trace("Set MD5 hash");
                                //setTransferFileMD5(workDirName + transfer_status_file, md5map);
                                pse = plugin.genGMessage(MsgEvent.Type.INFO, "Clinical Results Directory Transfer Complete");
                                pse.setParam("indir", workDirName);
                                pse.setParam("req_id", reqId);
                                pse.setParam("seq_id", seqId);
                                pse.setParam("transfer_status_file", transfer_status_file);
                                pse.setParam("bucket_name", bucket_name);
                                pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                                pse.setParam("pathstage", pathStage);
                                if (sampleList != null) {
                                    logger.trace("Samples : " + sampleList);
                                    pse.setParam("sample_list", sampleList);
                                } else {
                                    pse.setParam("sample_list", "");
                                }
                                pse.setParam("sstep", String.valueOf(sstep));
                                plugin.sendMsgEvent(pse);
                            }

                            logger.trace("Transferring Research Results Directory");
                            pse = plugin.genGMessage(MsgEvent.Type.INFO, "Transferring Research Results Directory");
                            pse.setParam("indir", workDirName);
                            pse.setParam("req_id", reqId);
                            pse.setParam("seq_id", seqId);
                            pse.setParam("transfer_status_file", transfer_status_file);
                            pse.setParam("bucket_name", bucket_name);
                            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                            pse.setParam("pathstage", pathStage);
                            pse.setParam("sstep", String.valueOf(sstep));
                            plugin.sendMsgEvent(pse);

                            oe.uploadSequenceDirectory(research_bucket_name, resultDirName + "research/" + seqId + "/", seqId, seqId, String.valueOf(sstep));

                            logger.trace("Checking if Research Results are Synced Properly");
                            pse = plugin.genGMessage(MsgEvent.Type.INFO, "Checking if Research Results are Synced Properly");
                            pse.setParam("indir", workDirName);
                            pse.setParam("req_id", reqId);
                            pse.setParam("seq_id", seqId);
                            pse.setParam("transfer_status_file", transfer_status_file);
                            pse.setParam("bucket_name", bucket_name);
                            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                            pse.setParam("pathstage", pathStage);
                            pse.setParam("sstep", String.valueOf(sstep));
                            plugin.sendMsgEvent(pse);

                            oe = new ObjectEngine(plugin);
                            if (oe.isSyncDir(research_bucket_name, seqId + "/", resultDirName + "research/" + seqId + "/", filterList)) {
                                logger.debug("Results Directory Sycned [inDir = {}]", resultDir);
                                //Map<String, String> md5map = oe.getDirMD5(workDirName, filterList);
                                //logger.trace("Set MD5 hash");
                                //setTransferFileMD5(workDirName + transfer_status_file, md5map);
                                pse = plugin.genGMessage(MsgEvent.Type.INFO, "Research Results Directory Transferred");
                                pse.setParam("indir", workDirName);
                                pse.setParam("req_id", reqId);
                                pse.setParam("seq_id", seqId);
                                pse.setParam("transfer_status_file", transfer_status_file);
                                pse.setParam("bucket_name", bucket_name);
                                pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                                pse.setParam("pathstage", pathStage);
                                pse.setParam("sstep", String.valueOf(sstep));
                                plugin.sendMsgEvent(pse);
                            }

                            logger.trace("Pre-processing is complete");
                            pse = plugin.genGMessage(MsgEvent.Type.INFO, "Pre-processing is complete");
                            pse.setParam("indir", workDirName);
                            pse.setParam("req_id", reqId);
                            pse.setParam("seq_id", seqId);
                            pse.setParam("transfer_status_file", transfer_status_file);
                            pse.setParam("bucket_name", bucket_name);
                            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                            pse.setParam("pathstage", pathStage);
                            pse.setParam("sstep", String.valueOf(sstep));
                            plugin.sendMsgEvent(pse);

                            break;
                        case 1:     // Container error encountered
                            pse = plugin.genGMessage(MsgEvent.Type.ERROR, "General Docker Error Encountered (Err: 1)");
                            pse.setParam("indir", workDirName);
                            pse.setParam("req_id", reqId);
                            pse.setParam("seq_id", seqId);
                            pse.setParam("transfer_status_file", transfer_status_file);
                            pse.setParam("bucket_name", bucket_name);
                            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                            pse.setParam("pathstage", pathStage);
                            pse.setParam("sstep", String.valueOf(sstep));
                            plugin.sendMsgEvent(pse);
                            break;
                        case 100:   // Script failure encountered
                            pse = plugin.genGMessage(MsgEvent.Type.ERROR, "Pre-processor encountered an error (Err: 100)");
                            pse.setParam("indir", workDirName);
                            pse.setParam("req_id", reqId);
                            pse.setParam("seq_id", seqId);
                            pse.setParam("transfer_status_file", transfer_status_file);
                            pse.setParam("bucket_name", bucket_name);
                            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                            pse.setParam("pathstage", pathStage);
                            pse.setParam("sstep", String.valueOf(sstep));
                            plugin.sendMsgEvent(pse);
                            break;
                        case 125:   // Container failed to run
                            pse = plugin.genGMessage(MsgEvent.Type.ERROR, "Container failed to run (Err: 125)");
                            pse.setParam("indir", workDirName);
                            pse.setParam("req_id", reqId);
                            pse.setParam("seq_id", seqId);
                            pse.setParam("transfer_status_file", transfer_status_file);
                            pse.setParam("bucket_name", bucket_name);
                            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                            pse.setParam("pathstage", pathStage);
                            pse.setParam("sstep", String.valueOf(sstep));
                            plugin.sendMsgEvent(pse);
                            break;
                        case 126:   // Container command cannot be invoked
                            pse = plugin.genGMessage(MsgEvent.Type.ERROR, "Container command failed to be invoked (Err: 126)");
                            pse.setParam("indir", workDirName);
                            pse.setParam("req_id", reqId);
                            pse.setParam("seq_id", seqId);
                            pse.setParam("transfer_status_file", transfer_status_file);
                            pse.setParam("bucket_name", bucket_name);
                            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                            pse.setParam("pathstage", pathStage);
                            pse.setParam("sstep", String.valueOf(sstep));
                            plugin.sendMsgEvent(pse);
                            break;
                        case 127:   // Container command cannot be found
                            pse = plugin.genGMessage(MsgEvent.Type.ERROR, "Container command could not be found (Err: 127)");
                            pse.setParam("indir", workDirName);
                            pse.setParam("req_id", reqId);
                            pse.setParam("seq_id", seqId);
                            pse.setParam("transfer_status_file", transfer_status_file);
                            pse.setParam("bucket_name", bucket_name);
                            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                            pse.setParam("pathstage", pathStage);
                            pse.setParam("sstep", String.valueOf(sstep));
                            plugin.sendMsgEvent(pse);
                            break;
                        case 137:   // Container was killed
                            pse = plugin.genGMessage(MsgEvent.Type.ERROR, "Container was manually stopped (Err: 137)");
                            pse.setParam("indir", workDirName);
                            pse.setParam("req_id", reqId);
                            pse.setParam("seq_id", seqId);
                            pse.setParam("transfer_status_file", transfer_status_file);
                            pse.setParam("bucket_name", bucket_name);
                            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                            pse.setParam("pathstage", pathStage);
                            pse.setParam("sstep", String.valueOf(sstep));
                            plugin.sendMsgEvent(pse);
                            break;
                        default:    // Other return code encountered
                            pse = plugin.genGMessage(MsgEvent.Type.ERROR, "Unknown container return code (Err: " + p.exitValue() + ")");
                            pse.setParam("indir", workDirName);
                            pse.setParam("req_id", reqId);
                            pse.setParam("seq_id", seqId);
                            pse.setParam("transfer_status_file", transfer_status_file);
                            pse.setParam("bucket_name", bucket_name);
                            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                            pse.setParam("pathstage", pathStage);
                            pse.setParam("sstep", String.valueOf(sstep));
                            plugin.sendMsgEvent(pse);
                            break;
                    }
                } else {
                    pse = plugin.genGMessage(MsgEvent.Type.ERROR, "Error retrieving return code from container");
                    pse.setParam("indir", workDirName);
                    pse.setParam("req_id", reqId);
                    pse.setParam("seq_id", seqId);
                    pse.setParam("transfer_status_file", transfer_status_file);
                    pse.setParam("bucket_name", bucket_name);
                    pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                    pse.setParam("pathstage", pathStage);
                    pse.setParam("sstep", String.valueOf(sstep));
                    plugin.sendMsgEvent(pse);
                }

                Process postClear = Runtime.getRuntime().exec("docker rm " + containerName);
                postClear.waitFor();
            } catch (Exception e) {
                logger.error("processSequence {}", e.getMessage());
                pse = plugin.genGMessage(MsgEvent.Type.ERROR, "Error Path Run");
                pse.setParam("req_id", reqId);
                pse.setParam("seq_id", seqId);
                pse.setParam("transfer_watch_file", transfer_watch_file);
                pse.setParam("transfer_status_file", transfer_status_file);
                pse.setParam("bucket_name", bucket_name);
                pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                pse.setParam("pathstage", pathStage);
                pse.setParam("error_message", e.getMessage());
                pse.setParam("sstep", String.valueOf(sstep));
                plugin.sendMsgEvent(pse);
            }
        }
        pstep = 2;
    }

    public void endProcessSequence(String seqId, String reqId) {
        MsgEvent pse;
        try {
            String containerName = "procseq." + seqId;

            Process kill = Runtime.getRuntime().exec("docker kill " + containerName);
            kill.waitFor();

            Thread.sleep(500);

            Process clear = Runtime.getRuntime().exec("docker rm " + containerName);
            clear.waitFor();

            if (kill.exitValue() == 0) {
                pse = plugin.genGMessage(MsgEvent.Type.INFO, "Pre-processing canceled");
                pse.setParam("req_id", reqId);
                pse.setParam("seq_id", seqId);
                pse.setParam("transfer_status_file", transfer_status_file);
                pse.setParam("bucket_name", bucket_name);
                pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                pse.setParam("pathstage", pathStage);
                pse.setParam("sstep", String.valueOf(0));
                plugin.sendMsgEvent(pse);
            }
        } catch (Exception e) {
            logger.error("processSequence {}", e.getMessage());
            pse = plugin.genGMessage(MsgEvent.Type.ERROR, "Error Path Run");
            pse.setParam("req_id", reqId);
            pse.setParam("seq_id", seqId);
            pse.setParam("transfer_watch_file", transfer_watch_file);
            pse.setParam("transfer_status_file", transfer_status_file);
            pse.setParam("bucket_name", bucket_name);
            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
            pse.setParam("pathstage", pathStage);
            pse.setParam("error_message", e.getMessage());
            pse.setParam("sstep", String.valueOf(0));
            plugin.sendMsgEvent(pse);
        }
    }

    private boolean deleteDirectory(File path) {
        boolean deleted = true;
        if (path.exists()) {
            File[] files = path.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory())
                        deleteDirectory(file);
                    else if (!file.delete())
                        deleted = false;
                }
            }
        }
        return (path.delete() && deleted);
    }

    private class PerfTracker extends Thread {
        private boolean isActive = false;
        private StringBuilder results = new StringBuilder();

        PerfTracker() {
            isActive = false;
        }

        public void run() {
            try {
                results.append("ts,cpu-idle-load,cpu-user-load,cpu-nice-load,cpu-sys-load,cpu-core-count-physical,cpu-core-count-logical,cpu-core-load,load-sane,memory-total,memory-available,memory-used,process-phase\n");
                isActive = true;
                Long perfRate = plugin.getConfig().getLongParam("perfrate", 5000L);
                while (isActive) {
                    logPerf();
                    Thread.sleep(perfRate);
                }
            } catch (Exception ex) {
                logger.error("PerfTracker run(): {}", ex.getMessage());
            }
        }

        public String getResults() {
            return results.toString();
        }

        private void logPerf() {

            MsgEvent me = plugin.getSysInfo();
            if (me != null) {

                /*
                Iterator it = me.getParams().entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry pairs = (Map.Entry) it.next();
                    logger.info(pairs.getKey() + " = " + pairs.getValue());
                    //String plugin = pairs.getKey().toString();
                }
                */

                //cpu-per-cpu-load = CPU Load per processor: 1.0% 12.0% 8.0% 7.9% 0.0% 0.0% 0.0% 0.0%
                //cpu-core-count = 8
                //String sCoreCountp = me.getParam("cpu-core-count-physical");
                //String sCoreCountl = me.getParam("cpu-core-count-logical");
                String sCoreCountp = me.getParam("cpu-core-count");
                String sCoreCountl = me.getParam("cpu-core-count");
                int pcoreCount = Integer.parseInt(sCoreCountp);
                String cpuPerLoad = me.getParam("cpu-per-cpu-load");
                cpuPerLoad = cpuPerLoad.substring(cpuPerLoad.indexOf(": ") + 2);
                cpuPerLoad = cpuPerLoad.replace("%", "");
                String[] perCpu = cpuPerLoad.split(" ");
                String sCputPerLoadGrp = "";
                for (String cpu : perCpu) {
                    //logger.info(cpu);
                    sCputPerLoadGrp += cpu + ":";
                }
                sCputPerLoadGrp = sCputPerLoadGrp.substring(0, sCputPerLoadGrp.length() - 1);

                String sMemoryTotal = me.getParam("memory-total");
                Long memoryTotal = Long.parseLong(sMemoryTotal);
                String sMemoryAvailable = me.getParam("memory-available");
                Long memoryAvailable = Long.parseLong(sMemoryAvailable);
                Long memoryUsed = memoryTotal - memoryAvailable;
                String sMemoryUsed = String.valueOf(memoryUsed);

                String sCpuIdleLoad = me.getParam("cpu-idle-load");
                String sCpuUserLoad = me.getParam("cpu-user-load");
                String sCpuNiceLoad = me.getParam("cpu-nice-load");
                String sCpuSysLoad = me.getParam("cpu-sys-load");
                float cpuIdleLoad = Float.parseFloat(sCpuIdleLoad);
                float cpuUserLoad = Float.parseFloat(sCpuUserLoad);
                float cpuNiceLoad = Float.parseFloat(sCpuNiceLoad);
                float cpuSysLoad = Float.parseFloat(sCpuSysLoad);
                float cpuTotalLoad = cpuIdleLoad + cpuUserLoad + cpuNiceLoad + cpuSysLoad;

                String smemoryUsed = String.valueOf(memoryUsed / 1024 / 1024);
                //String sCpuTotalLoad = String.valueOf(cpuTotalLoad);
                boolean loadIsSane = false;
                if (cpuTotalLoad == 100.0) {
                    loadIsSane = true;
                }

                //logger.info("MEM USED = " + smemoryUsed + " sTotalLoad = " + sCpuTotalLoad + " isSane = " + loadIsSane);

                //String header = "ts,cpu-idle-load,cpu-user-load,cpu-nice-load,cpu-sys-load,cpu-core-count-physical,cpu-core-count-logical,cpu-core-load,load-sane,memory-total,memory-available,memory-used,process-phase\n";
                String output = System.currentTimeMillis() + "," + sCpuIdleLoad + "," + sCpuUserLoad + "," + sCpuNiceLoad + "," + sCpuSysLoad + "," + sCoreCountp + "," + sCoreCountl + "," + sCputPerLoadGrp + "," + String.valueOf(loadIsSane) + "," + sMemoryTotal + "," + sMemoryAvailable + "," + sMemoryUsed + "," + stagePhase + "\n";
                results.append(output);

                /*String logPath = plugin.getConfig().getStringParam("perflogpath");
                if (logPath != null) {
                    try {
                        Path logpath = Paths.get(logPath);
                        //output += "\n";
                        if (!logpath.toFile().exists()) {
                            Files.write(logpath, header.getBytes(), StandardOpenOption.CREATE);
                            Files.write(logpath, output.getBytes(), StandardOpenOption.APPEND);
                        } else {
                            Files.write(logpath, output.getBytes(), StandardOpenOption.APPEND);
                        }

                    } catch (Exception e) {
                        logger.error("Error Static Runner " + e.getMessage());
                        e.printStackTrace();
                        //exception handling left as an exercise for the reader
                    }
                }*/

            } else {
                logger.error("PerfTracker logPerf() : me = null");
            }
        }
    }

    public void executeCommand(String inDir, String outDir, boolean trackPerf) {
        pstep = 3;
        //start perf mon
        PerfTracker pt = null;
        if (trackPerf) {
            pt = new PerfTracker();
            new Thread(pt).start();
        }

        //String command = "docker run -t -v /home/gpackage:/gpackage -v /home/gdata/input/160427_D00765_0033_AHKM2CBCXX/Sample3:/gdata/input -v /home/gdata/output/f8de921b-fdfa-4365-bf7d-39817b9d1883:/gdata/output  intrepo.uky.edu:5000/gbase /gdata/input/commands_main.sh";
        //String command = "docker run -t -v /home/gpackage:/gpackage -v " + tmpInput + ":/gdata/input -v " + tmpOutput + ":/gdata/output  intrepo.uky.edu:5000/gbase /gdata/input/commands_main.sh";
        String command = "docker run -t -v /home/gpackage:/gpackage -v " + inDir + ":/gdata/input -v " + outDir + ":/gdata/output  intrepo.uky.edu:5000/gbase /gdata/input/commands_main.sh";

        StringBuffer output = new StringBuffer();
        StringBuffer error = new StringBuffer();
        Process p;
        try {
            p = Runtime.getRuntime().exec(command);

            BufferedReader outputFeed = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String outputLine;
            long difftime = System.currentTimeMillis();
            while ((outputLine = outputFeed.readLine()) != null) {
                output.append(outputLine);

                String[] outputStr = outputLine.split("\\|\\|");

                //System.out.println(outputStr.length + ": " + outputLine);
                //for(String str : outputStr) {
                //System.out.println(outputStr.length + " " + str);
                //}
                for (int i = 0; i < outputStr.length; i++) {
                    outputStr[i] = outputStr[i].trim();
                }


                if ((outputStr.length == 5) && ((outputLine.toLowerCase().startsWith("info")) || (outputLine.toLowerCase().startsWith("error")))) {
                    Calendar cal = Calendar.getInstance();
                    SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.US);
                    cal.setTime(sdf.parse(outputStr[1].trim()));// all done

                    long logdiff = (cal.getTimeInMillis() - difftime);
                    difftime = cal.getTimeInMillis();

                    if (outputStr[0].toLowerCase().equals("info")) {
                        //logger.info("Log diff = " + logdiff + " : " +  outputStr[2] + " : " + outputStr[3] + " : " + outputStr[4]);
                        stagePhase = outputStr[3];
                    } else if (outputStr[0].toLowerCase().equals("error")) {
                        logger.error("Pipeline Error : " + outputLine.toString());
                    }
                }
                logger.debug(outputLine);

            }

            /*
            if (!output.toString().equals("")) {
                //INFO : Mon May  9 20:35:42 UTC 2016 : UKHC Genomics pipeline V-1.0 : run_secondary_analysis.pl : Module Function run_locally() - execution successful
                logger.info(output.toString());
                //    clog.info(output.toString());
            }
            BufferedReader errorFeed = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            String errorLine;
            while ((errorLine = errorFeed.readLine()) != null) {
                error.append(errorLine);
                logger.error(errorLine);
            }

            if (!error.toString().equals(""))
                logger.error(error.toString());
            //    clog.error(error.toString());
            */

            p.waitFor();
            if (trackPerf) {
                pt.isActive = false;
            }
        } catch (IOException ioe) {
            // WHAT!?! DO SOMETHIN'!
            logger.error(ioe.getMessage());
        } catch (InterruptedException ie) {
            // WHAT!?! DO SOMETHIN'!
            logger.error(ie.getMessage());
        } catch (Exception e) {
            // WHAT!?! DO SOMETHIN'!
            logger.error(e.getMessage());
        }
        pstep = 2;
    }

    public void processSample(String seqId, String sampleId, String reqId, boolean trackPerf) {
        logger.debug("Call to processSample seq_id: {}, sample_id: {}, req_id: {}", seqId, sampleId, reqId);

        pstep = 3;
        String results_bucket_name = plugin.getConfig().getStringParam("results_bucket");
        if (results_bucket_name == null || results_bucket_name.equals("")) {
            logger.error("Configuration value [results_bucket] is not properly set, halting agent");
            MsgEvent error = plugin.genGMessage(MsgEvent.Type.ERROR, "Configuration value [results_bucket] is not properly set");
            error.setParam("req_id", reqId);
            error.setParam("seq_id", seqId);
            error.setParam("sample_id", sampleId);
            error.setParam("transfer_status_file", transfer_status_file);
            error.setParam("bucket_name", bucket_name);
            error.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
            error.setParam("pathstage", pathStage);
            error.setParam("ssstep", "0");
            plugin.sendMsgEvent(error);
            plugin.PathProcessorActive = false;
            return;
        }

        MsgEvent pse;
        int ssstep = 1;
        String workDirName = null;
        try {
            workDirName = incoming_directory; //create random tmp location
            workDirName = workDirName.replace("//", "/");
            if (!workDirName.endsWith("/")) {
                workDirName += "/";
            }
            File workDir = new File(workDirName);
            if (workDir.exists()) {
                deleteDirectory(workDir);
            }
            workDir.mkdir();


            String remoteDir = seqId + "/" + sampleId + "/";

            ObjectEngine oe = new ObjectEngine(plugin);

            oe.createBucket(results_bucket_name);

            pse = plugin.genGMessage(MsgEvent.Type.INFO, "Directory Transfering");
            //me.setParam("inDir", remoteDir);
            //me.setParam("outDir", incoming_directory);
            pse.setParam("req_id", reqId);
            pse.setParam("seq_id", seqId);
            pse.setParam("sample_id", sampleId);
            pse.setParam("transfer_status_file", transfer_status_file);
            pse.setParam("bucket_name", bucket_name);
            pse.setParam("results_bucket_name", results_bucket_name);
            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
            pse.setParam("pathstage", pathStage);
            pse.setParam("ssstep", String.valueOf(ssstep));
            plugin.sendMsgEvent(pse);

            oe.downloadDirectory(bucket_name, remoteDir, workDirName, seqId, sampleId);

            workDirName += remoteDir;

            File commands_main = new File(workDirName + "commands_main.sh");

            if (!commands_main.exists()) {
                MsgEvent error = plugin.genGMessage(MsgEvent.Type.ERROR, "Commands file is missing from download");
                error.setParam("req_id", reqId);
                error.setParam("seq_id", seqId);
                error.setParam("sample_id", sampleId);
                error.setParam("transfer_status_file", transfer_status_file);
                error.setParam("bucket_name", bucket_name);
                error.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                error.setParam("pathstage", pathStage);
                error.setParam("ssstep", String.valueOf(ssstep));
                plugin.sendMsgEvent(error);
                return;
            }

            commands_main.setExecutable(true);

            List<String> filterList = new ArrayList<>();
            logger.trace("Add [transfer_status_file] to [filterList]");
            /*
            filterList.add(transfer_status_file);
            String inDir = incoming_directory;
            if (!inDir.endsWith("/")) {
                inDir = inDir + "/";
            }
            */

            //logger.debug("[inDir = {}]", inDir);
            oe = new ObjectEngine(plugin);
            if (oe.isSyncDir(bucket_name, remoteDir, workDirName, filterList)) {
                ssstep = 2;
                logger.debug("Directory Sycned [inDir = {}]", workDirName);
                //Map<String, String> md5map = oe.getDirMD5(workDirName, filterList);
                //logger.trace("Set MD5 hash");
                //setTransferFileMD5(workDirName + transfer_status_file, md5map);
                pse = plugin.genGMessage(MsgEvent.Type.INFO, "Directory Transfered");
                pse.setParam("indir", workDirName);
                pse.setParam("req_id", reqId);
                pse.setParam("seq_id", seqId);
                pse.setParam("sample_id", sampleId);
                pse.setParam("transfer_status_file", transfer_status_file);
                pse.setParam("bucket_name", bucket_name);
                pse.setParam("results_bucket_name", results_bucket_name);
                pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                pse.setParam("pathstage", pathStage);
                pse.setParam("ssstep", String.valueOf(ssstep));
                plugin.sendMsgEvent(pse);
                ssstep = 3;
            }
        } catch (Exception ex) {
            logger.error("run {}", ex.getMessage());
            pse = plugin.genGMessage(MsgEvent.Type.ERROR, "Error Path Run");
            pse.setParam("req_id", reqId);
            pse.setParam("seq_id", seqId);
            pse.setParam("sample_id", sampleId);
            pse.setParam("transfer_watch_file", transfer_watch_file);
            pse.setParam("transfer_status_file", transfer_status_file);
            pse.setParam("bucket_name", bucket_name);
            pse.setParam("results_bucket_name", results_bucket_name);
            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
            pse.setParam("pathstage", pathStage);
            pse.setParam("error_message", ex.getMessage());
            pse.setParam("ssstep", String.valueOf(ssstep));
            plugin.sendMsgEvent(pse);
        }

        //if is makes it through process the sample
        if (ssstep == 3) {
            try {
                //start perf mon
                PerfTracker pt = null;
                if (trackPerf) {
                    logger.trace("Starting performance monitoring");
                    pt = new PerfTracker();
                    new Thread(pt).start();
                }

                pse = plugin.genGMessage(MsgEvent.Type.INFO, "Creating output directory");
                pse.setParam("indir", workDirName);
                pse.setParam("req_id", reqId);
                pse.setParam("seq_id", seqId);
                pse.setParam("sample_id", sampleId);
                pse.setParam("transfer_status_file", transfer_status_file);
                pse.setParam("bucket_name", bucket_name);
                pse.setParam("results_bucket_name", results_bucket_name);
                pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                pse.setParam("pathstage", pathStage);
                pse.setParam("ssstep", String.valueOf(ssstep));
                plugin.sendMsgEvent(pse);

                String resultDirName = outgoing_directory; //create random tmp location
                resultDirName = resultDirName.replace("//", "/");
                if (!resultDirName.endsWith("/")) {
                    resultDirName += "/";
                }
                File resultDir = new File(resultDirName);
                if (resultDir.exists()) {
                    deleteDirectory(resultDir);
                }
                logger.trace("Creating output directory: {}", resultDirName);
                resultDir.mkdir();

                ssstep = 4;
                pse = plugin.genGMessage(MsgEvent.Type.INFO, "Starting Pipeline via Docker Container");
                pse.setParam("indir", workDirName);
                pse.setParam("outdir", resultDirName);
                pse.setParam("req_id", reqId);
                pse.setParam("seq_id", seqId);
                pse.setParam("sample_id", sampleId);
                pse.setParam("transfer_status_file", transfer_status_file);
                pse.setParam("bucket_name", bucket_name);
                pse.setParam("results_bucket_name", results_bucket_name);
                pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                pse.setParam("pathstage", pathStage);
                pse.setParam("ssstep", String.valueOf(ssstep));
                plugin.sendMsgEvent(pse);

                String containerName = "procsam." + sampleId.replace("/", ".");

                Process kill = Runtime.getRuntime().exec("docker kill " + containerName);
                kill.waitFor();

                Thread.sleep(500);

                Process clear = Runtime.getRuntime().exec("docker rm " + containerName);
                clear.waitFor();

                String command = "docker run -t -v /mnt/localdata/gpackage:/gpackage " +
                        "-v " + workDirName + ":/gdata/input " +
                        "-v " + resultDirName + ":/gdata/output " +
                        "--name procsam." + sampleId.replace("/", ".") + " " +
                        "intrepo.uky.edu:5000/gbase /gdata/input/commands_main.sh";

                logger.trace("Running Docker Command: {}", command);

                StringBuilder output = new StringBuilder();
                Process p = null;
                try {
                    p = Runtime.getRuntime().exec(command);
                    logger.trace("Attaching output reader");
                    BufferedReader outputFeed = new BufferedReader(new InputStreamReader(p.getInputStream()));
                    String outputLine;
                    while ((outputLine = outputFeed.readLine()) != null) {
                        output.append(outputLine);
                        output.append("\n");

                        String[] outputStr = outputLine.split("\\|\\|");

                        for (int i = 0; i < outputStr.length; i++) {
                            outputStr[i] = outputStr[i].trim();
                        }

                        if ((outputStr.length == 5) &&
                                ((outputLine.toLowerCase().startsWith("info")) ||
                                 (outputLine.toLowerCase().startsWith("error")))) {
                            if (outputStr[0].toLowerCase().equals("info")) {
                                if (!stagePhase.equals(outputStr[3])) {
                                    pse = plugin.genGMessage(MsgEvent.Type.INFO, "Pipeline now in phase " + outputStr[3]);
                                    pse.setParam("indir", workDirName);
                                    pse.setParam("outdir", resultDirName);
                                    pse.setParam("req_id", reqId);
                                    pse.setParam("seq_id", seqId);
                                    pse.setParam("sample_id", sampleId);
                                    pse.setParam("transfer_status_file", transfer_status_file);
                                    pse.setParam("bucket_name", bucket_name);
                                    pse.setParam("results_bucket_name", results_bucket_name);
                                    pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                                    pse.setParam("pathstage", pathStage);
                                    pse.setParam("ssstep", String.valueOf(ssstep));
                                    plugin.sendMsgEvent(pse);
                                }
                                stagePhase = outputStr[3];
                            } else if (outputStr[0].toLowerCase().equals("error")) {
                                logger.error("Pipeline Error : " + outputLine);
                                pse = plugin.genGMessage(MsgEvent.Type.ERROR, "");
                                pse.setParam("indir", workDirName);
                                pse.setParam("outdir", resultDirName);
                                pse.setParam("req_id", reqId);
                                pse.setParam("seq_id", seqId);
                                pse.setParam("sample_id", sampleId);
                                pse.setParam("transfer_status_file", transfer_status_file);
                                pse.setParam("bucket_name", bucket_name);
                                pse.setParam("results_bucket_name", results_bucket_name);
                                pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                                pse.setParam("pathstage", pathStage);
                                pse.setParam("ssstep", String.valueOf(ssstep));
                                pse.setParam("error_message", outputLine);
                                plugin.sendMsgEvent(pse);
                            }
                        }
                        logger.debug(outputLine);
                    }
                    logger.trace("Waiting for Docker process to finish");

                    p.waitFor();
                    logger.trace("Docker exit code = {}", p.exitValue());

                    if (trackPerf) {
                        logger.trace("Ending Performance Monitor");
                        pt.isActive = false;
                        logger.trace("Sending Performance Information");
                        pse = plugin.genGMessage(MsgEvent.Type.INFO, "Sending Performance Information");
                        pse.setParam("indir", workDirName);
                        pse.setParam("req_id", reqId);
                        pse.setParam("seq_id", seqId);
                        pse.setParam("sample_id", sampleId);
                        pse.setParam("transfer_status_file", transfer_status_file);
                        pse.setParam("bucket_name", bucket_name);
                        pse.setParam("results_bucket_name", results_bucket_name);
                        pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                        pse.setParam("pathstage", pathStage);
                        pse.setParam("ssstep", String.valueOf(ssstep));
                        pse.setParam("perf_log", pt.getResults());
                        plugin.sendMsgEvent(pse);
                    }
                    pse = plugin.genGMessage(MsgEvent.Type.INFO, "Sending Pipeline Output");
                    pse.setParam("indir", workDirName);
                    pse.setParam("req_id", reqId);
                    pse.setParam("seq_id", seqId);
                    pse.setParam("sample_id", sampleId);
                    pse.setParam("transfer_status_file", transfer_status_file);
                    pse.setParam("bucket_name", bucket_name);
                    pse.setParam("results_bucket_name", results_bucket_name);
                    pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                    pse.setParam("pathstage", pathStage);
                    pse.setParam("ssstep", String.valueOf(ssstep));
                    pse.setParam("output_log", output.toString());
                    plugin.sendMsgEvent(pse);
                } catch (IOException ioe) {
                    // WHAT!?! DO SOMETHIN'!
                    logger.error("File read/write exception: {}", ioe.getMessage());
                } catch (InterruptedException ie) {
                    // WHAT!?! DO SOMETHIN'!
                    logger.error("Process was interrupted: {}", ie.getMessage());
                } catch (Exception e) {
                    // WHAT!?! DO SOMETHIN'!
                    logger.error("Exception: {}", e.getMessage());
                }

                logger.trace("Pipeline has finished");

                Thread.sleep(2000);

                if (p != null) {
                    switch (p.exitValue()) {
                        case 0:     // Container finished successfully
                            ssstep = 5;
                            pse = plugin.genGMessage(MsgEvent.Type.INFO, "Pipeline has completed");
                            pse.setParam("indir", workDirName);
                            pse.setParam("req_id", reqId);
                            pse.setParam("seq_id", seqId);
                            pse.setParam("sample_id", sampleId);
                            pse.setParam("transfer_status_file", transfer_status_file);
                            pse.setParam("bucket_name", bucket_name);
                            pse.setParam("results_bucket_name", results_bucket_name);
                            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                            pse.setParam("pathstage", pathStage);
                            pse.setParam("ssstep", String.valueOf(ssstep));
                            plugin.sendMsgEvent(pse);

                            ObjectEngine oe = new ObjectEngine(plugin);

                            pse = plugin.genGMessage(MsgEvent.Type.INFO, "Cleaning out old results from Object Store");
                            //me.setParam("inDir", remoteDir);
                            //me.setParam("outDir", incoming_directory);
                            pse.setParam("req_id", reqId);
                            pse.setParam("seq_id", seqId);
                            pse.setParam("sample_id", sampleId);
                            pse.setParam("transfer_status_file", transfer_status_file);
                            pse.setParam("bucket_name", bucket_name);
                            pse.setParam("results_bucket_name", results_bucket_name);
                            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                            pse.setParam("pathstage", pathStage);
                            pse.setParam("ssstep", String.valueOf(ssstep));
                            plugin.sendMsgEvent(pse);

                            oe.deleteBucketDirectoryContents(results_bucket_name, sampleId + "/");

                            logger.trace("Uploading results to objectStore");

                            ssstep = 6;
                            pse = plugin.genGMessage(MsgEvent.Type.INFO, "Uploading Results Directory");
                            pse.setParam("indir", workDirName);
                            pse.setParam("req_id", reqId);
                            pse.setParam("seq_id", seqId);
                            pse.setParam("sample_id", sampleId);
                            pse.setParam("transfer_status_file", transfer_status_file);
                            pse.setParam("bucket_name", bucket_name);
                            pse.setParam("results_bucket_name", results_bucket_name);
                            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                            pse.setParam("pathstage", pathStage);
                            pse.setParam("ssstep", String.valueOf(ssstep));
                            plugin.sendMsgEvent(pse);

                            oe.uploadSampleDirectory(results_bucket_name, resultDirName, "", seqId, sampleId, String.valueOf(ssstep));

                            List<String> filterList = new ArrayList<>();
                            filterList.add("pipeline_log.txt");
                            logger.trace("Add [transfer_status_file] to [filterList]");

                            logger.trace("Sleeping to ensure completion message is received last");
                            Thread.sleep(2000);

                            oe = new ObjectEngine(plugin);
                            if (oe.isSyncDir(results_bucket_name, "", resultDirName, filterList)) {
                                ssstep = 7;
                                logger.debug("Results Directory Sycned [inDir = {}]", resultDirName);
                                //Map<String, String> md5map = oe.getDirMD5(workDirName, filterList);
                                //logger.trace("Set MD5 hash");
                                //setTransferFileMD5(workDirName + transfer_status_file, md5map);
                                pse = plugin.genGMessage(MsgEvent.Type.INFO, "Results Directory Transferred");
                                pse.setParam("indir", workDirName);
                                pse.setParam("req_id", reqId);
                                pse.setParam("seq_id", seqId);
                                pse.setParam("sample_id", sampleId);
                                pse.setParam("transfer_status_file", transfer_status_file);
                                pse.setParam("bucket_name", bucket_name);
                                pse.setParam("results_bucket_name", results_bucket_name);
                                pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                                pse.setParam("pathstage", pathStage);
                                pse.setParam("ssstep", String.valueOf(ssstep));
                                plugin.sendMsgEvent(pse);
                            }
                            break;
                        case 1:     // Container error encountered
                            pse = plugin.genGMessage(MsgEvent.Type.ERROR, "General Docker Error Encountered (Err: 1)");
                            pse.setParam("indir", workDirName);
                            pse.setParam("req_id", reqId);
                            pse.setParam("seq_id", seqId);
                            pse.setParam("sample_id", sampleId);
                            pse.setParam("transfer_status_file", transfer_status_file);
                            pse.setParam("bucket_name", bucket_name);
                            pse.setParam("results_bucket_name", results_bucket_name);
                            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                            pse.setParam("pathstage", pathStage);
                            pse.setParam("ssstep", String.valueOf(ssstep));
                            plugin.sendMsgEvent(pse);
                            break;
                        case 100:   // Script failure encountered
                            pse = plugin.genGMessage(MsgEvent.Type.ERROR, "Processing Pipeline encountered an error (Err: 100)");
                            pse.setParam("indir", workDirName);
                            pse.setParam("req_id", reqId);
                            pse.setParam("seq_id", seqId);
                            pse.setParam("sample_id", sampleId);
                            pse.setParam("transfer_status_file", transfer_status_file);
                            pse.setParam("bucket_name", bucket_name);
                            pse.setParam("results_bucket_name", results_bucket_name);
                            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                            pse.setParam("pathstage", pathStage);
                            pse.setParam("ssstep", String.valueOf(ssstep));
                            plugin.sendMsgEvent(pse);
                            break;
                        case 125:   // Container failed to run
                            pse = plugin.genGMessage(MsgEvent.Type.ERROR, "Container failed to run (Err: 125)");
                            pse.setParam("indir", workDirName);
                            pse.setParam("req_id", reqId);
                            pse.setParam("seq_id", seqId);
                            pse.setParam("sample_id", sampleId);
                            pse.setParam("transfer_status_file", transfer_status_file);
                            pse.setParam("bucket_name", bucket_name);
                            pse.setParam("results_bucket_name", results_bucket_name);
                            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                            pse.setParam("pathstage", pathStage);
                            pse.setParam("ssstep", String.valueOf(ssstep));
                            plugin.sendMsgEvent(pse);
                            break;
                        case 126:   // Container command cannot be invoked
                            pse = plugin.genGMessage(MsgEvent.Type.ERROR, "Container command failed to be invoked (Err: 126)");
                            pse.setParam("indir", workDirName);
                            pse.setParam("req_id", reqId);
                            pse.setParam("seq_id", seqId);
                            pse.setParam("sample_id", sampleId);
                            pse.setParam("transfer_status_file", transfer_status_file);
                            pse.setParam("bucket_name", bucket_name);
                            pse.setParam("results_bucket_name", results_bucket_name);
                            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                            pse.setParam("pathstage", pathStage);
                            pse.setParam("ssstep", String.valueOf(ssstep));
                            plugin.sendMsgEvent(pse);
                            break;
                        case 127:   // Container command cannot be found
                            pse = plugin.genGMessage(MsgEvent.Type.ERROR, "Container command could not be found (Err: 127)");
                            pse.setParam("indir", workDirName);
                            pse.setParam("req_id", reqId);
                            pse.setParam("seq_id", seqId);
                            pse.setParam("sample_id", sampleId);
                            pse.setParam("transfer_status_file", transfer_status_file);
                            pse.setParam("bucket_name", bucket_name);
                            pse.setParam("results_bucket_name", results_bucket_name);
                            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                            pse.setParam("pathstage", pathStage);
                            pse.setParam("ssstep", String.valueOf(ssstep));
                            plugin.sendMsgEvent(pse);
                            break;
                        case 137:   // Container was killed
                            pse = plugin.genGMessage(MsgEvent.Type.ERROR, "Container was manually stopped (Err: 137)");
                            pse.setParam("indir", workDirName);
                            pse.setParam("req_id", reqId);
                            pse.setParam("seq_id", seqId);
                            pse.setParam("sample_id", sampleId);
                            pse.setParam("transfer_status_file", transfer_status_file);
                            pse.setParam("bucket_name", bucket_name);
                            pse.setParam("results_bucket_name", results_bucket_name);
                            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                            pse.setParam("pathstage", pathStage);
                            pse.setParam("ssstep", String.valueOf(ssstep));
                            plugin.sendMsgEvent(pse);
                            break;
                        default:    // Other return code encountered
                            pse = plugin.genGMessage(MsgEvent.Type.ERROR, "Unknown container return code (Err: " + p.exitValue() + ")");
                            pse.setParam("indir", workDirName);
                            pse.setParam("req_id", reqId);
                            pse.setParam("seq_id", seqId);
                            pse.setParam("sample_id", sampleId);
                            pse.setParam("transfer_status_file", transfer_status_file);
                            pse.setParam("bucket_name", bucket_name);
                            pse.setParam("results_bucket_name", results_bucket_name);
                            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                            pse.setParam("pathstage", pathStage);
                            pse.setParam("ssstep", String.valueOf(ssstep));
                            plugin.sendMsgEvent(pse);
                            break;
                    }
                } else {
                    pse = plugin.genGMessage(MsgEvent.Type.ERROR, "Error retrieving exit code of container");
                    pse.setParam("indir", workDirName);
                    pse.setParam("req_id", reqId);
                    pse.setParam("seq_id", seqId);
                    pse.setParam("sample_id", sampleId);
                    pse.setParam("transfer_status_file", transfer_status_file);
                    pse.setParam("bucket_name", bucket_name);
                    pse.setParam("results_bucket_name", results_bucket_name);
                    pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                    pse.setParam("pathstage", pathStage);
                    pse.setParam("ssstep", String.valueOf(ssstep));
                    plugin.sendMsgEvent(pse);
                }

                Thread.sleep(500);

                Process postClear = Runtime.getRuntime().exec("docker rm " + containerName);
                postClear.waitFor();
            } catch (Exception e) {
                logger.error("processSample {}", e.getMessage());
                pse = plugin.genGMessage(MsgEvent.Type.ERROR, "Error Path Run");
                pse.setParam("req_id", reqId);
                pse.setParam("seq_id", seqId);
                pse.setParam("sample_id", sampleId);
                pse.setParam("transfer_watch_file", transfer_watch_file);
                pse.setParam("transfer_status_file", transfer_status_file);
                pse.setParam("bucket_name", bucket_name);
                pse.setParam("results_bucket_name", results_bucket_name);
                pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                pse.setParam("pathstage", pathStage);
                pse.setParam("error_message", e.getMessage());
                pse.setParam("ssstep", String.valueOf(ssstep));
                plugin.sendMsgEvent(pse);
            }
        }
        pstep = 2;
    }

    public void endProcessSample(String seqId, String sampleId, String reqId) {
        MsgEvent pse;
        try {
            String containerName = "procsam." + sampleId.replace("/", ".");

            Process kill = Runtime.getRuntime().exec("docker kill " + containerName);
            kill.waitFor();

            Thread.sleep(500);

            Process clear = Runtime.getRuntime().exec("docker rm " + containerName);
            clear.waitFor();

            if (kill.exitValue() == 0) {
                pse = plugin.genGMessage(MsgEvent.Type.INFO, "Pre-processing canceled");
                pse.setParam("req_id", reqId);
                pse.setParam("seq_id", seqId);
                pse.setParam("sample_id", sampleId);
                pse.setParam("transfer_status_file", transfer_status_file);
                pse.setParam("bucket_name", bucket_name);
                pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                pse.setParam("pathstage", pathStage);
                pse.setParam("ssstep", String.valueOf(0));
                plugin.sendMsgEvent(pse);
            }
        } catch (Exception e) {
            logger.error("processSequence {}", e.getMessage());
            pse = plugin.genGMessage(MsgEvent.Type.ERROR, "Error Path Run");
            pse.setParam("req_id", reqId);
            pse.setParam("seq_id", seqId);
            pse.setParam("sample_id", sampleId);
            pse.setParam("transfer_watch_file", transfer_watch_file);
            pse.setParam("transfer_status_file", transfer_status_file);
            pse.setParam("bucket_name", bucket_name);
            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
            pse.setParam("pathstage", pathStage);
            pse.setParam("error_message", e.getMessage());
            pse.setParam("ssstep", String.valueOf(0));
            plugin.sendMsgEvent(pse);
        }
    }

    public void downloadResults(String seqId, String reqId) {
        logger.debug("Call to downloadResults seq_id: " + seqId, ", req_id: " + reqId);

        pstep = 3;
        int sstep = 1;

        String remoteDir = seqId + "/";

        MsgEvent pse;

        try {
            String workDirName = incoming_directory; //create random tmp location
            workDirName = workDirName.replace("//", "/");
            if (!workDirName.endsWith("/")) {
                workDirName += "/";
            }

            List<String> filterList = new ArrayList<>();
            logger.trace("Add [transfer_status_file] to [filterList]");
            /*
            filterList.add(transfer_status_file);
            String inDir = incoming_directory;
            if (!inDir.endsWith("/")) {
                inDir = inDir + "/";
            }

            //workDirName += remoteDir;
            */
            File seqDir = new File(workDirName + seqId);
            if (seqDir.exists()) {
                deleteDirectory(seqDir);
            }
            //workDir.mkdir();

            ObjectEngine oe = new ObjectEngine(plugin);

            pse = plugin.genGMessage(MsgEvent.Type.INFO, "Directory Transfering");
            //me.setParam("inDir", remoteDir);
            //me.setParam("outDir", incoming_directory);
            pse.setParam("seq_id", seqId);
            pse.setParam("req_id", reqId);
            pse.setParam("transfer_status_file", transfer_status_file);
            pse.setParam("bucket_name", bucket_name);
            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
            pse.setParam("pathstage", pathStage);
            pse.setParam("sstep", String.valueOf(sstep));
            plugin.sendMsgEvent(pse);
            sstep = 2;

            oe.downloadDirectory(bucket_name, remoteDir, workDirName, seqId, null);

            //logger.debug("[inDir = {}]", inDir);
            oe = new ObjectEngine(plugin);
            if (oe.isSyncDir(bucket_name, remoteDir, workDirName + seqId, filterList)) {
                logger.debug("Directory Sycned [inDir = {}]", workDirName);
                //Map<String, String> md5map = oe.getDirMD5(inDir, filterList);
                //logger.trace("Set MD5 hash");
                //setTransferFileMD5(inDir + transfer_status_file, md5map);
                pse = plugin.genGMessage(MsgEvent.Type.INFO, "Directory Transfered");
                pse.setParam("indir", workDirName);
                pse.setParam("seq_id", seqId);
                pse.setParam("req_id", reqId);
                pse.setParam("transfer_status_file", transfer_status_file);
                pse.setParam("bucket_name", bucket_name);
                pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                pse.setParam("pathstage", pathStage);
                pse.setParam("sstep", String.valueOf(sstep));
                plugin.sendMsgEvent(pse);
                sstep = 3;
            }
        } catch (Exception ex) {
            logger.error("run {}", ex.getMessage());
            pse = plugin.genGMessage(MsgEvent.Type.ERROR, "Error Path Run");
            pse.setParam("seq_id", seqId);
            pse.setParam("req_id", reqId);
            pse.setParam("transfer_watch_file", transfer_watch_file);
            pse.setParam("transfer_status_file", transfer_status_file);
            pse.setParam("bucket_name", bucket_name);
            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
            pse.setParam("pathstage", pathStage);
            pse.setParam("error_message", ex.getMessage());
            pse.setParam("sstep", String.valueOf(sstep));
            plugin.sendMsgEvent(pse);
        }
        pstep = 2;
    }

    private String getSampleList(String inDir) {
        String sampleList = null;
        try {
            if (!inDir.endsWith("/")) {
                inDir += "/";
            }
            ArrayList<String> samples = new ArrayList<>();
            logger.trace("Processing Sequence Directory : " + inDir);
            File file = new File(inDir);
            String[] directories = file.list(new FilenameFilter() {
                @Override
                public boolean accept(File current, String name) {
                    return new File(current, name).isDirectory();
                }
            });

            List<String> subDirectories = new ArrayList<>();

            if (directories != null) {
                for (String subDir : directories) {
                    logger.trace("Searching for sub-directories of {}", inDir +  subDir);
                    subDirectories.add(subDir);
                    File subFile = new File(inDir + "/" + subDir);
                    String[] subSubDirs = subFile.list(new FilenameFilter() {
                        @Override
                        public boolean accept(File current, String name) {
                            return new File(current, name).isDirectory();
                        }
                    });
                    if (subSubDirs != null) {
                        for (String subSubDir : subSubDirs) {
                            logger.trace("Found sub-directory {}", inDir +  subDir + "/" + subSubDir);
                            subDirectories.add(subDir + "/" + subSubDir);
                        }
                    }
                }
            }

            for (String subDirectory : subDirectories) {
                logger.trace("Processing Sample SubDirectory : " + subDirectory);
                String commands_main_filename = inDir + subDirectory + "/commands_main.sh";
                String config_files_directoryname = inDir + subDirectory + "/config_files";
                File commands_main = new File(commands_main_filename);
                File config_files = new File(config_files_directoryname);
                logger.trace("commands_main " + commands_main_filename + " exist : " + commands_main.exists());
                logger.trace("config_files " + config_files_directoryname + " exist : " + config_files.exists());

                if (commands_main.exists() && !commands_main.isDirectory() &&
                        config_files.exists() && config_files.isDirectory()) {
                    // do something
                    samples.add(subDirectory);
                    logger.trace("Found Sample: " + commands_main_filename + " and " + config_files_directoryname);
                }
            }
            //build list
            if (!samples.isEmpty()) {
                sampleList = "";
                for (String tSample : samples) {
                    sampleList += tSample + ",";
                }
                sampleList = sampleList.substring(0, sampleList.length() - 1);
            }
        } catch (Exception ex) {
            logger.error("getSameplList : " + ex.getMessage());
        }
        return sampleList;
    }

    private void sendUpdateInfoMessage(String seqId, String sampleId, String reqId, String step, String message) {
        if (!message.equals("Idle"))
            logger.info("{}", message);
        MsgEvent msgEvent = plugin.genGMessage(MsgEvent.Type.INFO, message);
        msgEvent.setParam("pathstage", String.valueOf(plugin.pathStage));
        msgEvent.setParam("seq_id", seqId);
        if (sampleId != null) {
            msgEvent.setParam("sample_id", sampleId);
            msgEvent.setParam("ssstep", step);
        } else if (seqId != null)
            msgEvent.setParam("sstep", step);
        else
            msgEvent.setParam("pstep", step);
        if (reqId != null)
            msgEvent.setParam("req_id", reqId);
        plugin.sendMsgEvent(msgEvent);
    }

    private void sendUpdateErrorMessage(String seqId, String sampleId, String reqId, String step, String message) {
        logger.error("{}", message);
        MsgEvent msgEvent = plugin.genGMessage(MsgEvent.Type.ERROR, "");
        msgEvent.setParam("pathstage", String.valueOf(plugin.pathStage));
        msgEvent.setParam("error_message", message);
        msgEvent.setParam("seq_id", seqId);
        if (sampleId != null) {
            msgEvent.setParam("sample_id", sampleId);
            msgEvent.setParam("ssstep", step);
        } else if (seqId != null)
            msgEvent.setParam("sstep", step);
        else
            msgEvent.setParam("pstep", step);
        if (reqId != null)
            msgEvent.setParam("req_id", reqId);
        plugin.sendMsgEvent(msgEvent);
    }

    /*
    private void legacy() {
        logger.trace("Thread starting");
        try {
            logger.trace("Setting [PathProcessorActive] to true");
            plugin.PathProcessorActive = true;
            ObjectEngine oe = new ObjectEngine(plugin);
            logger.trace("Entering while-loop");
            while (plugin.PathProcessorActive) {
                me = plugin.genGMessage(MsgEvent.Type.INFO,"Start Object Scan");
                me.setParam("transfer_watch_file",transfer_watch_file);
                me.setParam("transfer_status_file", transfer_status_file);
                me.setParam("bucket_name",bucket_name);
                me.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                me.setParam("pathstage",pathStage);
                me.setParam("pstep","2");
                plugin.sendMsgEvent(me);

                try {
                    //oe.deleteBucketContents(bucket_name);
                    logger.trace("Populating [remoteDirs]");
                    List<String> remoteDirs = oe.listBucketDirs(bucket_name);
                    for(String remoteDir : remoteDirs) {
                        logger.trace("Remote Dir : " + remoteDir);
                    }
                    logger.trace("Populating [localDirs]");
                    List<String> localDirs = getWalkPath(incoming_directory);
                    for(String localDir : localDirs) {
                        logger.trace("Local Dir : " + localDir);
                    }

                    List<String> newDirs = new ArrayList<>();
                    for (String remoteDir : remoteDirs) {
                        logger.trace("Checking for existance of RemoteDir [" + remoteDir + "] locally");
                        if (!localDirs.contains(remoteDir)) {
                            logger.trace("RemoteDir [" + remoteDir + "] does not exist locally");
                            if (oe.doesObjectExist(bucket_name, remoteDir + transfer_watch_file)) {
                                logger.debug("Adding [remoteDir = {}] to [newDirs]", remoteDir);
                                newDirs.add(remoteDir);
                            }
                        }
                    }
                    if (!newDirs.isEmpty()) {
                        logger.trace("[newDirs] has buckets to process");
                        processBucket(newDirs);
                    }
                    Thread.sleep(30000);
                } catch (Exception ex) {
                    logger.error("run : while {}", ex.getMessage());
                    me = plugin.genGMessage(MsgEvent.Type.ERROR,"Error during Object scan");
                    me.setParam("transfer_watch_file",transfer_watch_file);
                    me.setParam("transfer_status_file", transfer_status_file);
                    me.setParam("bucket_name",bucket_name);
                    me.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                    me.setParam("pathstage",pathStage);
                    me.setParam("error_message",ex.getMessage());
                    me.setParam("pstep","2");
                    plugin.sendMsgEvent(me);
                }
                //message end of scan
                me = plugin.genGMessage(MsgEvent.Type.INFO,"End Object Scan");
                me.setParam("transfer_watch_file",transfer_watch_file);
                me.setParam("transfer_status_file", transfer_status_file);
                me.setParam("bucket_name",bucket_name);
                me.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                me.setParam("pathstage",pathStage);
                me.setParam("pstep","3");
                plugin.sendMsgEvent(me);

            }
        } catch (Exception ex) {
            logger.error("run {}", ex.getMessage());
            me = plugin.genGMessage(MsgEvent.Type.ERROR,"Error Path Run");
            me.setParam("transfer_watch_file",transfer_watch_file);
            me.setParam("transfer_status_file", transfer_status_file);
            me.setParam("bucket_name",bucket_name);
            me.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
            me.setParam("pathstage",pathStage);
            me.setParam("error_message",ex.getMessage());
            me.setParam("pstep","2");
            plugin.sendMsgEvent(me);
        }

    }
    private void processBucket(List<String> newDirs) {
        logger.debug("Call to processBucket [newDir = {}]", newDirs.toString());
        ObjectEngine oe = new ObjectEngine(plugin);

        for (String remoteDir : newDirs) {
            logger.debug("Downloading directory {} to [incoming_directory]", remoteDir);

            String seqId = remoteDir.substring(remoteDir.lastIndexOf("/") + 1, remoteDir.length());

            me = plugin.genGMessage(MsgEvent.Type.INFO,"Directory Transfered");
            me.setParam("inDir", remoteDir);
            me.setParam("outDir", incoming_directory);
            me.setParam("seq_id", seqId);
            me.setParam("transfer_watch_file",transfer_watch_file);
            me.setParam("transfer_status_file", transfer_status_file);
            me.setParam("bucket_name",bucket_name);
            me.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
            me.setParam("pathstage",pathStage);
            me.setParam("sstep","1");
            plugin.sendMsgEvent(me);

            oe.downloadDirectory(bucket_name, remoteDir, incoming_directory);

            List<String> filterList = new ArrayList<>();
            logger.trace("Add [transfer_status_file] to [filterList]");
            filterList.add(transfer_status_file);
            String inDir = incoming_directory;
            if (!inDir.endsWith("/")) {
                inDir = inDir + "/";
            }
            inDir = inDir + remoteDir;
            logger.debug("[inDir = {}]", inDir);
            oe = new ObjectEngine(plugin);
            if (oe.isSyncDir(bucket_name, remoteDir, inDir, filterList)) {
                logger.debug("Directory Sycned [inDir = {}]", inDir);
                Map<String, String> md5map = oe.getDirMD5(inDir, filterList);
                logger.trace("Set MD5 hash");
                setTransferFileMD5(inDir + transfer_status_file, md5map);
                me = plugin.genGMessage(MsgEvent.Type.INFO,"Directory Transfered");
                me.setParam("indir", inDir);
                me.setParam("outdir", remoteDir);
                me.setParam("seq_id", seqId);
                me.setParam("transfer_watch_file",transfer_watch_file);
                me.setParam("transfer_status_file", transfer_status_file);
                me.setParam("bucket_name",bucket_name);
                me.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                me.setParam("pathstage",pathStage);
                me.setParam("sstep","2");
                plugin.sendMsgEvent(me);
            }
        }

    }
*/
    private void setTransferFileMD5(String dir, Map<String, String> md5map) {
        logger.debug("Call to setTransferFileMD5 [dir = {}]", dir);
        try {
            PrintWriter out = null;
            try {
                logger.trace("Opening [dir] to write");
                out = new PrintWriter(new BufferedWriter(new FileWriter(dir, true)));
                for (Map.Entry<String, String> entry : md5map.entrySet()) {
                    String md5file = entry.getKey().replace(incoming_directory, "");
                    if (md5file.startsWith("/")) {
                        md5file = md5file.substring(1);
                    }
                    out.write(md5file + ":" + entry.getValue() + "\n");
                    logger.debug("[md5file = {}, entry = {}] written", md5file, entry.getValue());
                }
            } finally {
                try {
                    assert out != null;
                    out.flush();
                    out.close();
                } catch (AssertionError e) {
                    logger.error("setTransferFileMd5 - PrintWriter was pre-emptively shutdown");
                }
            }
        } catch (Exception ex) {
            logger.error("setTransferFile {}", ex.getMessage());
        }
    }

    /*private List<String> getWalkPath(String path) {
        logger.debug("Call to getWalkPath [path = {}]", path);
        if (!path.endsWith("/")) {
            path = path + "/";
        }
        List<String> dirList = new ArrayList<>();

        File root = new File(path);
        File[] list = root.listFiles();

        if (list == null) {
            logger.trace("[list] is null, returning [dirList (empty array)]");
            return dirList;
        }

        for (File f : list) {
            if (f.isDirectory()) {
                //walkPath( f.getAbsolutePath() );
                String dir = f.getAbsolutePath().replace(path, "");
                logger.debug("Adding \"{}/\" to [dirList]", dir);
                dirList.add(dir + "/");
            }
        }
        return dirList;
    }*/
}



