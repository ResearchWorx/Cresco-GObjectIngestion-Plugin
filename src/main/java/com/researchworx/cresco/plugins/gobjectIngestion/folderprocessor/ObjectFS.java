package com.researchworx.cresco.plugins.gobjectIngestion.folderprocessor;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import com.researchworx.cresco.plugins.gobjectIngestion.Plugin;
import com.researchworx.cresco.plugins.gobjectIngestion.objectstorage.Encapsulation;
import com.researchworx.cresco.plugins.gobjectIngestion.objectstorage.ObjectEngine;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.*;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;

public class ObjectFS implements Runnable {

    private final String transfer_watch_file;
    private final String transfer_status_file;

    private String bucket_name;
    //private String raw_bucket_name;
    //private String clinical_bucket_name;
    //private String research_bucket_name;
    //private String results_bucket_name;
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

        bucket_name = plugin.getConfig().getStringParam("raw_bucket");
        /*raw_bucket_name = plugin.getConfig().getStringParam("raw_bucket");
        logger.debug("\"pathstage" + pathStage + "\" --> \"raw_bucket\" from config [{}]", raw_bucket_name);
        clinical_bucket_name = plugin.getConfig().getStringParam("clinical_bucket");
        logger.debug("\"pathstage" + pathStage + "\" --> \"clinical_bucket\" from config [{}]", clinical_bucket_name);
        research_bucket_name = plugin.getConfig().getStringParam("research_bucket");
        logger.debug("\"pathstage" + pathStage + "\" --> \"research_bucket\" from config [{}]", research_bucket_name);
        results_bucket_name = plugin.getConfig().getStringParam("results_bucket");
        logger.debug("\"pathstage" + pathStage + "\" --> \"results_bucket\" from config [{}]", results_bucket_name);*/

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
            Plugin.setActive();
            logger.trace("Entering while-loop");
            while (Plugin.processorIsActive()) {
                sendUpdateInfoMessage(null, null, null, String.valueOf(pstep), "Idle");
                Thread.sleep(plugin.getConfig().getIntegerParam("scan_interval", 5000));
            }
        } catch (Exception e) {
            sendUpdateErrorMessage(null, null, null, String.valueOf(pstep),
                    String.format("General Run Exception [%s:%s]", e.getClass().getCanonicalName(), e.getMessage()));
        }
    }

    public void testBaggedSequenceDownload(String seqId, String reqId, boolean trackPerf) {
        logger.debug("testBaggedSequenceDownload('{}','{}',{})", seqId, reqId, trackPerf);
        ObjectEngine oe = new ObjectEngine(plugin);
        pstep = 3;
        int sstep = 0;
        String raw_bucket_name = plugin.getConfig().getStringParam("raw_bucket");
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
            if (oe.downloadBaggedDirectory(raw_bucket_name, remoteDir, workDirName, seqId, null, reqId,
                    String.valueOf(sstep))) {
                File baggedSequenceFile = new File(workDirName + seqId + ".tar");
                sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                        String.format("Download successful, unboxing sequence file [%s]", baggedSequenceFile));

                if (!Encapsulation.unarchive(baggedSequenceFile, workDir)) {
                    sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                            String.format("Failed to unarchive sequence file [%s]", baggedSequenceFile.getAbsolutePath()));
                    pstep = 2;
                    return;
                }
                String unboxed = workDirName + seqId + "/";
                if (!new File(unboxed).exists() || !new File(unboxed).isDirectory()) {
                    sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                            String.format("Unboxing to [%s] failed", unboxed));
                    pstep = 2;
                    return;
                }
                logger.trace("unBoxIt result: {}, deleting TAR file", unboxed);
                baggedSequenceFile.delete();
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
        pstep = 2;
    }

    public void testPreProcessSequence(String seqId, String reqId, boolean trackPerf) {
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
            sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                    "Retrieving bagged sequence");
            sstep = 2;
            if (new File(workDirName + seqId + "/").exists())
                sstep = 3;
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

    public void testSampleDataUpload(String seqId, String reqId, boolean trackPerf) {
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
            sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                    "Retrieving bagged sequence");
            sstep = 2;
            if (new File(workDirName + seqId + "/").exists())
                sstep = 3;
        } catch (Exception e) {
            sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                    String.format("Exception encountered: [%s:%s]", e.getClass().getCanonicalName(), e.getMessage()));
        }
        sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                "Creating output directory");
        String resultDirName = outgoing_directory; //create random tmp location
        logger.trace("Clearing/resetting output directory");
        resultDirName = resultDirName.replace("//", "/");
        if (!resultDirName.endsWith("/")) {
            resultDirName += "/";
        }
        String clinicalResultsDirName = resultDirName + "clinical/";
        String researchResultsDirName = resultDirName + "research/";
        sstep = 5;
        sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                "Pipeline has completed");
        sstep = 6;
        if (new File(clinicalResultsDirName + seqId + "/").exists()) {
            sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                    "Transferring Clinical Results Directory");
            String sampleList = getSampleList(resultDirName + "clinical/" + seqId + "/");
            sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                    String.format("Sample list: %s", sampleList));
            String samples[] = sampleList.split(",");
            for (String sample : samples) {
                oe = new ObjectEngine(plugin);
                try {
                    if (oe.uploadBaggedDirectory(clinical_bucket_name, clinicalResultsDirName +
                            seqId + "/" + sample, seqId, seqId, null, reqId, String.valueOf(sstep))) {
                        sendUpdateInfoMessage(seqId, sample, reqId, String.valueOf(sstep),
                                String.format("Uploaded [%s] to [%s]", sample, clinical_bucket_name));
                    }
                } catch (Exception e) {
                    sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                        String.format("processBaggedSequence exception encountered [%s:%s]\n%s",
                                e.getClass().getCanonicalName(), e.getMessage(), ExceptionUtils.getStackTrace(e)));
                }
            }
        }
        pstep = 2;
    }

    public void processBaggedSequence(String seqId, String reqId, boolean trackPerf) {
        logger.debug("processBaggedSequence('{}','{}',{})", seqId, reqId, trackPerf);
        ObjectEngine oe = new ObjectEngine(plugin);
        pstep = 3;
        int sstep = 0;
        String raw_bucket_name = plugin.getConfig().getStringParam("raw_bucket");
        String clinical_bucket_name = plugin.getConfig().getStringParam("clinical_bucket");
        String research_bucket_name = plugin.getConfig().getStringParam("research_bucket");
        logger.trace("Checking to see if raw bucket [{}] exists", raw_bucket_name);
        if (raw_bucket_name == null || raw_bucket_name.equals("") ||
                !oe.doesBucketExist(raw_bucket_name)) {
            sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                    String.format("Raw bucket [%s] does not exist",
                            raw_bucket_name != null ? raw_bucket_name : "NULL"));
            plugin.PathProcessorActive = false;
            return;
        }
        logger.trace("Checking to see if clinical bucket [{}] exists", clinical_bucket_name);
        if (clinical_bucket_name == null || clinical_bucket_name.equals("") ||
                !oe.doesBucketExist(clinical_bucket_name)) {
            sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                    String.format("Clinical bucket [%s] does not exist",
                            clinical_bucket_name != null ? clinical_bucket_name : "NULL"));
            plugin.PathProcessorActive = false;
            return;
        }
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
            if (oe.downloadBaggedDirectory(raw_bucket_name, remoteDir, workDirName, seqId, null, reqId,
                    String.valueOf(sstep))) {
                File baggedSequenceFile = new File(workDirName + seqId + ".tar");
                sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                        String.format("Download successful, unboxing sequence file [%s]", baggedSequenceFile));
                /*if (!Encapsulation.unBoxIt(baggedSequenceFile)) {
                    sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                            String.format("Failed to unbox sequence file [%s]", baggedSequenceFile));
                    pstep = 2;
                    return;
                }*/
                if (!Encapsulation.unarchive(baggedSequenceFile, workDir)) {
                    sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                            String.format("Failed to unarchive sequence file [%s]", baggedSequenceFile.getAbsolutePath()));
                    pstep = 2;
                    return;
                }
                String unboxed = workDirName + seqId + "/";
                if (!new File(unboxed).exists() || !new File(unboxed).isDirectory()) {
                    sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                            String.format("Unboxing to [%s] failed", unboxed));
                    pstep = 2;
                    return;
                }
                logger.trace("unBoxIt result: {}, deleting TAR file", unboxed);
                baggedSequenceFile.delete();
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
                            if (new File(resultDirName + "clinical/" + seqId + "/").exists()) {
                                String sampleList = getSampleList(resultDirName + "clinical/" + seqId + "/");
                                if (sampleList != null) {
                                    sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                                            String.format("Transferring Clinical Results Directory. Sample list: %s", sampleList));
                                    String samples[] = sampleList.split(",");
                                    boolean uploaded = true;
                                    for (String sample : samples) {
                                        oe = new ObjectEngine(plugin);
                                        try {
                                            if (oe.uploadBaggedDirectory(clinical_bucket_name, clinicalResultsDirName +
                                                    seqId + "/" + sample, seqId, seqId, null, reqId, String.valueOf(sstep))) {
                                                sendUpdateInfoMessage(seqId, sample, reqId, String.valueOf(sstep),
                                                        String.format("Uploaded [%s] to [%s]", sample, clinical_bucket_name));
                                            } else {
                                                sendUpdateErrorMessage(seqId, sample, reqId, String.valueOf(sstep),
                                                        String.format("Failed to upload [%s]", sample));
                                                uploaded = false;
                                            }
                                        } catch (Exception e) {
                                            sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                                                    String.format("processBaggedSequence exception encountered [%s:%s]\n%s",
                                                            e.getClass().getCanonicalName(), e.getMessage(), ExceptionUtils.getStackTrace(e)));
                                        }
                                    }
                                    if (uploaded) {
                                        sstep = 7;
                                        pse = plugin.genGMessage(MsgEvent.Type.INFO,
                                                "Clinical Results Directory Transfer Complete");
                                        pse.setParam("req_id", reqId);
                                        pse.setParam("seq_id", seqId);
                                        pse.setParam("pathstage", pathStage);
                                        pse.setParam("sample_list", sampleList);
                                        pse.setParam("sstep", String.valueOf(sstep));
                                        plugin.sendMsgEvent(pse);
                                    }
                                } else {
                                    sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                                            "No samples found");
                                    sstep = 8;
                                }
                            } else {
                                sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                                        "No clinical results generated");
                                sstep = 8;
                            }
                            if (new File(resultDirName + "research/" + seqId + "/").exists()) {
                                sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                                        "Transferring Research Results Directory");
                                if (oe.uploadBaggedDirectory(research_bucket_name, resultDirName + "research/" +
                                        seqId + "/", seqId, seqId, null, reqId, String.valueOf(sstep))) {
                                    sstep = 9;
                                    logger.debug("Results Directory Sycned [inDir = {}]", resultDir);
                                    pse = plugin.genGMessage(MsgEvent.Type.INFO,
                                            "Research Results Directory Transferred");
                                    pse.setParam("req_id", reqId);
                                    pse.setParam("seq_id", seqId);
                                    pse.setParam("pathstage", pathStage);
                                    pse.setParam("sstep", String.valueOf(sstep));
                                    plugin.sendMsgEvent(pse);
                                } else {
                                    sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                                            String.format("Failed to upload research results [%s]", seqId));
                                }
                            } else {
                                sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                                        "No research results generated");
                            }
                            sstep = 10;
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

    public void preprocessBaggedSequence(String seqId, String reqId, boolean trackPerf) {
        logger.debug("preprocessBaggedSequence('{}','{}',{})", seqId, reqId, trackPerf);
        pstep = 3;
        int sstep = 0;
        String raw_bucket_name = plugin.getConfig().getStringParam("raw_bucket");
        String clinical_bucket_name = plugin.getConfig().getStringParam("clinical_bucket");
        String research_bucket_name = plugin.getConfig().getStringParam("research_bucket");
        String incoming_directory = plugin.getConfig().getStringParam("incoming_directory");
        String outgoing_directory = plugin.getConfig().getStringParam("outgoing_directory");
        String container_name = plugin.getConfig().getStringParam("container_name");
        try {
            sendUpdateInfoMessage(seqId, null, reqId, sstep, "Starting to preprocess sequence");
            Thread.sleep(1000);
            if (!preprocessBaggedSequenceCheckAndPrepare(seqId, reqId, sstep, raw_bucket_name, clinical_bucket_name,
                    research_bucket_name, incoming_directory, outgoing_directory, container_name)) {
                Thread.sleep(1000);
                sendUpdateErrorMessage(seqId, null, reqId, sstep,
                        "Failed sequence preprocessor initialization");
                pstep = 2;
                return;
            }
            sstep++;
        } catch (InterruptedException ie) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep, "Preprocessor initialization was interrupted");
        } catch (Exception e) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                    String.format("preprocessBaggedSequence exception encountered - %s", ExceptionUtils.getStackTrace(e)));
        }
        try {
            Thread.sleep(1000);
            if (!preprocessBaggedSequenceDownloadSequence(seqId, reqId, sstep, raw_bucket_name,
                    incoming_directory)) {
                Thread.sleep(1000);
                sendUpdateErrorMessage(seqId, null, reqId, sstep, "Failed to download sequence file");
                pstep = 2;
                return;
            }
            sstep++;
        } catch (InterruptedException ie) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep, "Sequence download was interrupted");
        } catch (Exception e) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                    String.format("preprocessBaggedSequence exception encountered - %s", ExceptionUtils.getStackTrace(e)));
        }
        try {
            File workDir = new File(incoming_directory);
            File resultsDir = new File(outgoing_directory);
            Thread.sleep(1000);
            int retCode = preprocessBaggedSequenceRunContainer(seqId, reqId, sstep, workDir, resultsDir,
                    container_name, trackPerf);
            if (retCode != 0) {
                Thread.sleep(1000);
                if (retCode == -1)
                    sendUpdateErrorMessage(seqId, null, reqId, sstep, "Failed to process sample");
                else {
                    switch (retCode) {
                        case 1:     // Container error encountered
                            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                                    "General Docker Error Encountered (Err: 1)");
                            break;
                        case 100:   // Script failure encountered
                            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                                    "Processing Pipeline encountered an error (Err: 100)");
                            break;
                        case 125:   // Container failed to run
                            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                                    "Container failed to run (Err: 125)");
                            break;
                        case 126:   // Container command cannot be invoked
                            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                                    "Container command failed to be invoked (Err: 126)");
                            break;
                        case 127:   // Container command cannot be found
                            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                                    "Container command could not be found (Err: 127)");
                            break;
                        case 137:   // Container was killed
                            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                                    "Container was manually stopped (Err: 137)");
                            break;
                        default:    // Other return code encountered
                            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                                    String.format("Unknown container return code (Err: %d)", retCode));
                            break;
                    }
                }
                pstep = 2;
                return;
            }
            sstep++;
        } catch (InterruptedException ie) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep, "Sequence preprocessing was interrupted");
        } catch (Exception e) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                    String.format("preprocessBaggedSequence exception encountered - %s", ExceptionUtils.getStackTrace(e)));
        }
        try {
            File resultsDir = new File(outgoing_directory);
            Thread.sleep(1000);
            if (!preprocessBaggedSequenceUploadResults(seqId, reqId, sstep, resultsDir, clinical_bucket_name,
                    research_bucket_name)) {
                Thread.sleep(1000);
                sendUpdateErrorMessage(seqId, null, reqId, sstep, "Failed to upload sequence results");
                pstep = 2;
                return;
            }
            sstep++;
            Thread.sleep(1000);
            sendUpdateInfoMessage(seqId, null, reqId, sstep, "Sequence results upload complete");
        } catch (InterruptedException ie) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep, "Sequence results upload was interrupted");
        } catch (Exception e) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                    String.format("preprocessBaggedSequence exception encountered - %s", ExceptionUtils.getStackTrace(e)));
        }
        pstep = 2;
    }

    private boolean preprocessBaggedSequenceCheckAndPrepare(String seqId, String reqId, int sstep, String raw_bucket_name,
                                                         String clinical_bucket_name, String research_bucket_name,
                                                         String incoming_directory, String outgoing_directory,
                                                         String container_name) {
        logger.debug("preprocessBaggedSequenceCheckAndPrepare('{}','{}',{})", seqId, reqId, sstep);
        sendUpdateInfoMessage(seqId, null, reqId, sstep, "Checking and preparing sequence preprocessor");
        try {
            ObjectEngine oe = new ObjectEngine(plugin);
            if (raw_bucket_name == null || raw_bucket_name.equals("")) {
                sendUpdateErrorMessage(seqId, null, reqId, sstep,
                        "Raw bucket is not properly set on this processor");
                Plugin.setInactive();
                return false;
            }
            if (clinical_bucket_name == null || clinical_bucket_name.equals("")) {
                sendUpdateErrorMessage(seqId, null, reqId, sstep,
                        "Clinical bucket is not properly set on this processor");
                Plugin.setInactive();
                return false;
            }
            if (research_bucket_name == null || research_bucket_name.equals("")) {
                sendUpdateErrorMessage(seqId, null, reqId, sstep,
                        "Research bucket is not properly set on this processor");
                Plugin.setInactive();
                return false;
            }
            if (incoming_directory == null || incoming_directory.equals("")) {
                sendUpdateErrorMessage(seqId, null, reqId, sstep,
                        "Incoming (working) directory is not properly set on this processor");
                Plugin.setInactive();
                return false;
            }
            if (outgoing_directory == null || outgoing_directory.equals("")) {
                sendUpdateErrorMessage(seqId, null, reqId, sstep,
                        "Outgoing (results) directory is not properly set on this processor");
                Plugin.setInactive();
                return false;
            }
            if (container_name == null || container_name.equals("")) {
                sendUpdateErrorMessage(seqId, null, reqId, sstep,
                        "Docker container name is not properly set on this processor");
                Plugin.setInactive();
                return false;
            }
            logger.trace("Checking to see if clinical bucket [{}] exists", clinical_bucket_name);
            if (!oe.doesBucketExist(clinical_bucket_name)) {
                sendUpdateErrorMessage(seqId, null, reqId, sstep,
                        String.format("Clinical bucket [%s] does not exist for given S3 credentials", clinical_bucket_name));
                return false;
            }
            logger.trace("Checking to see if research bucket [{}] exists", research_bucket_name);
            if (!oe.doesBucketExist(research_bucket_name)) {
                sendUpdateErrorMessage(seqId, null, reqId, sstep,
                        String.format("Results bucket [%s] does not exist for given S3 credentials", research_bucket_name));
                return false;
            }
            File workDir = new File(incoming_directory);
            if (workDir.exists())
                if (!deleteDirectory(workDir)) {
                    sendUpdateErrorMessage(seqId, null, reqId, sstep,
                            String.format("Failed to remove existing work directory [%s]", incoming_directory));
                    return false;
                }
            if (!workDir.mkdirs()) {
                sendUpdateErrorMessage(seqId, null, reqId, sstep,
                        String.format("Failed to create work directory [%s]", incoming_directory));
                return false;
            }
            File resultsDir = new File(outgoing_directory);
            if (resultsDir.exists())
                if (!deleteDirectory(resultsDir)) {
                    sendUpdateErrorMessage(seqId, null, reqId, sstep,
                            String.format("Failed to remove existing results directory [%s]",
                                    resultsDir.getAbsolutePath()));
                    return false;
                }
            if (!resultsDir.mkdirs()) {
                sendUpdateErrorMessage(seqId, null, reqId, sstep,
                        String.format("Failed to create results directory [%s]", resultsDir.getAbsolutePath()));
                return false;
            }
            File clinicalResultsDir = Paths.get(resultsDir.getAbsolutePath(), "clinical").toFile();
            if (!clinicalResultsDir.mkdirs()) {
                sendUpdateErrorMessage(seqId, null, reqId, sstep,
                        String.format("Failed to create clinical results directory [%s]",
                                clinicalResultsDir.getAbsolutePath()));
                return false;
            }
            File researchResultsDir = Paths.get(resultsDir.getAbsolutePath(), "research").toFile();
            if (!researchResultsDir.mkdirs()) {
                sendUpdateErrorMessage(seqId, null, reqId, sstep,
                        String.format("Failed to create research results directory [%s]",
                                researchResultsDir.getAbsolutePath()));
                return false;
            }
            return true;
        } catch (AmazonServiceException ase) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                    String.format("Caught an AmazonServiceException, which means your request made it " +
                            "to Amazon S3, but was rejected with an error response for some reason - %s", ase.getMessage()));
            return false;
        } catch (SdkClientException ace) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                    String.format("Caught an AmazonClientException, which means the client encountered "
                            + "a serious internal problem while trying to communicate with S3, "
                            + "such as not being able to access the network - %s", ace.getMessage()));
            return false;
        } catch (Exception e) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                    String.format("Check and prepare exception encountered - %s", ExceptionUtils.getStackTrace(e)));
            return false;
        }
    }

    private boolean preprocessBaggedSequenceDownloadSequence(String seqId, String reqId, int sstep,
                                                      String raw_bucket_name, String incoming_directory) {
        logger.debug("preprocessBaggedSequenceDownloadSequence('{}','{}','{}',{})", seqId, reqId, sstep);
        sendUpdateInfoMessage(seqId, null, reqId, sstep, "Downloading sequence file");
        try {
            ObjectEngine oe = new ObjectEngine(plugin);
            if (!oe.downloadBaggedDirectory(raw_bucket_name, seqId, incoming_directory, seqId, null,
                    reqId, String.valueOf(sstep))) {
                sendUpdateErrorMessage(seqId, null, reqId, sstep, "Failed to download sequence raw file");
                return false;
            }
            File baggedSequenceFile = Paths.get(incoming_directory, seqId + ObjectEngine.extension).toFile();
            if (!baggedSequenceFile.exists()) {
                sendUpdateErrorMessage(seqId, null, reqId, sstep, "Failed to download sequence raw file");
                return false;
            }
            sendUpdateInfoMessage(seqId, null, reqId, sstep,
                    String.format("Download successful, unboxing sequence file [%s]", baggedSequenceFile.getAbsolutePath()));
            if (!Encapsulation.unarchive(baggedSequenceFile, new File(incoming_directory))) {
                sendUpdateErrorMessage(seqId, null, reqId, sstep,
                        String.format("Failed to unarchive sequence file [%s]", baggedSequenceFile.getAbsolutePath()));
                return false;
            }
            File unboxed = Paths.get(incoming_directory, seqId).toFile();
            if (!unboxed.exists() || !unboxed.isDirectory()) {
                sendUpdateErrorMessage(seqId, null, reqId, sstep,
                        String.format("Unboxing to [%s] failed", unboxed));
                return false;
            }
            if (!baggedSequenceFile.delete()) {
                sendUpdateErrorMessage(seqId, null, reqId, sstep,
                        String.format("Failed to delete sample archive file [%s]", baggedSequenceFile.getAbsolutePath()));
            }
            sendUpdateInfoMessage(seqId, null, reqId, sstep,
                    String.format("Validating sample [%s]", unboxed.getAbsolutePath()));
            if (!Encapsulation.isBag(unboxed.getAbsolutePath())) {
                sendUpdateErrorMessage(seqId, null, reqId, sstep,
                        String.format("Unboxed sequence [%s] does not contain BagIt data", unboxed.getAbsolutePath()));
                return false;
            }
            if (!Encapsulation.verifyBag(unboxed.getAbsolutePath(), true)) {
                sendUpdateErrorMessage(seqId, null, reqId, sstep,
                        String.format("Unboxed sequence [%s] failed BagIt verification", unboxed.getAbsolutePath()));
                return false;
            }
            sendUpdateInfoMessage(seqId, null, reqId, sstep,
                    String.format("Restoring sample [%s]", unboxed.getAbsolutePath()));
            Encapsulation.debagify(unboxed.getAbsolutePath());
            sendUpdateInfoMessage(seqId, null, reqId, sstep,
                    String.format("Sequence [%s] restored to [%s]", seqId, unboxed));
            return true;
        } catch (AmazonServiceException ase) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                    String.format("Caught an AmazonServiceException, which means your request made it " +
                            "to Amazon S3, but was rejected with an error response for some reason - %s", ase.getMessage()));
            return false;
        } catch (SdkClientException ace) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                    String.format("Caught an AmazonClientException, which means the client encountered "
                            + "a serious internal problem while trying to communicate with S3, "
                            + "such as not being able to access the network - %s", ace.getMessage()));
            return false;
        } catch (Exception e) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                    String.format("Sequence download exception encountered - %s", ExceptionUtils.getStackTrace(e)));
            return false;
        }
    }

    private int preprocessBaggedSequenceRunContainer(String seqId, String reqId, int sstep, File workDir,
                                                     File resultsDir, String container, boolean trackPerf) {
        sendUpdateInfoMessage(seqId, null, reqId, sstep, "Starting pipeline via Docker container");
        String containerName = "procseq." + seqId;
        try {
            Process kill = Runtime.getRuntime().exec("docker kill " + containerName);
            kill.waitFor();
            Thread.sleep(500);
            Process clear = Runtime.getRuntime().exec("docker rm " + containerName);
            clear.waitFor();
        } catch (InterruptedException ie) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                    "Interrupted while clearing out existing containers");
            return -1;
        } catch (IOException ioe) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                    String.format("Failed to execute shell command(s) to clean out existing containers "
                            + "- %s", ExceptionUtils.getStackTrace(ioe)));
            return -1;
        }
        File clinicalResultsDir = Paths.get(resultsDir.getAbsolutePath(), "clinical").toFile();
        File researchResultsDir = Paths.get(resultsDir.getAbsolutePath(), "research").toFile();
        String command = String.format("docker run -v %s:/gdata/output/clinical -v %s:/gdata/output/research " +
                "-v %s:/gdata/input -e INPUT_FOLDER_PATH=/gdata/input/%s --name %s " +
                "-t %s /opt/pretools/raw_data_processing.pl", clinicalResultsDir.getAbsolutePath(),
                researchResultsDir.getAbsolutePath(), workDir.getAbsolutePath(), seqId, containerName, container);
        sendUpdateInfoMessage(seqId, null, reqId, sstep, String.format("Executing command: %s", command));

        PerfTracker pt = null;
        if (trackPerf) {
            logger.trace("Starting performance monitoring");
            pt = new PerfTracker();
            new Thread(pt).start();
        }
        StringBuilder output = new StringBuilder();
        Process p;
        try {
            p = Runtime.getRuntime().exec(command);
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
                        if (!stagePhase.equals(outputStr[3]))
                            sendUpdateInfoMessage(seqId, null, reqId, sstep,
                                    String.format("Pipeline is now in phase: %s", outputStr[3]));
                        stagePhase = outputStr[3];
                    } else if (outputStr[0].toLowerCase().equals("error"))
                        sendUpdateErrorMessage(seqId, null, reqId, sstep,
                                String.format("Pipeline error: %s", outputLine));
                }
                logger.debug(outputLine);
            }
            logger.trace("Waiting for Docker process to finish");
            p.waitFor();
            logger.trace("Docker exit code = {}", p.exitValue());
            MsgEvent pse;
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
            Thread.sleep(2000);
            sendUpdateInfoMessage(seqId, null, reqId, sstep, "Pipeline has completed");
            Thread.sleep(500);
            Process postClear = Runtime.getRuntime().exec("docker rm " + containerName);
            postClear.waitFor();
            return p.exitValue();
        } catch (IOException ioe) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                    String.format("Run container I/O exception encountered - %s", ExceptionUtils.getStackTrace(ioe)));
            return -1;
        } catch (InterruptedException ie) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                    String.format("Container interrupted - %s", ExceptionUtils.getStackTrace(ie)));
            return -1;
        } catch (Exception e) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                    String.format("Run container exception encountered - %s", ExceptionUtils.getStackTrace(e)));
            return -1;
        }
    }

    private boolean preprocessBaggedSequenceUploadResults(String seqId, String reqId, int sstep, File resultsDir,
                                                          String clinicalBucketName, String researchBucketName) {
        try {
            File clinicalResultsDir = Paths.get(resultsDir.getAbsolutePath(), "clinical", seqId).toFile();
            if (clinicalResultsDir.exists()) {
                String sampleList = getSampleList(clinicalResultsDir.getAbsolutePath());
                if (sampleList != null) {
                    sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                            String.format("Transferring Clinical Results Directory. Sample list: %s", sampleList));
                    String samples[] = sampleList.split(",");
                    boolean uploaded = true;
                    for (String sample : samples) {
                        String samplePath = Paths.get(clinicalResultsDir.getAbsolutePath(), sample).toString();
                        ObjectEngine oe = new ObjectEngine(plugin);
                        try {
                            if (oe.uploadBaggedDirectory(clinicalBucketName, samplePath, seqId, seqId,
                                    null, reqId, String.valueOf(sstep))) {
                                sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                                        String.format("Uploaded [%s] to [%s]", sample, clinicalBucketName));
                            } else {
                                sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                                        String.format("Failed to upload [%s]", sample));
                                uploaded = false;
                            }
                        } catch (Exception e) {
                            sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                                    String.format("processBaggedSequence exception encountered [%s:%s]\n%s",
                                            e.getClass().getCanonicalName(), e.getMessage(),
                                            ExceptionUtils.getStackTrace(e)));
                            return false;
                        }
                    }
                    if (uploaded) {
                        MsgEvent pse = plugin.genGMessage(MsgEvent.Type.INFO,
                                "Clinical Results Directory Transfer Complete");
                        pse.setParam("req_id", reqId);
                        pse.setParam("seq_id", seqId);
                        pse.setParam("pathstage", pathStage);
                        pse.setParam("sample_list", sampleList);
                        pse.setParam("sstep", String.valueOf(sstep));
                        plugin.sendMsgEvent(pse);
                    } else {
                        sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                                "Some sample(s) failed to upload");
                        return false;
                    }
                } else {
                    sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                            "No samples found in clinical results folder");
                }
            } else {
                sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                        "No clinical results generated");
            }
            File researchResultsDir = Paths.get(resultsDir.getAbsolutePath(), "research", seqId).toFile();
            if (researchResultsDir.exists()) {
                ObjectEngine oe = new ObjectEngine(plugin);
                sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                    "Transferring Research Results Directory");
                if (oe.uploadBaggedDirectory(researchBucketName, researchResultsDir.getAbsolutePath(), seqId, seqId,
                        null, reqId, String.valueOf(sstep))) {
                    sendUpdateInfoMessage(seqId, null, reqId, sstep,
                            "Research results directory uploaded");
                } else {
                    sendUpdateErrorMessage(seqId, null, reqId, String.valueOf(sstep),
                            String.format("Failed to upload research results [%s]", seqId));
                    return false;
                }
            } else {
                sendUpdateInfoMessage(seqId, null, reqId, String.valueOf(sstep),
                        "No research results generated");
            }
            return true;
        } catch (Exception e) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                    String.format("Results upload exception encountered - %s", ExceptionUtils.getStackTrace(e)));
            return false;
        }
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

    public void processBaggedSample(String seqId, String sampleId, String reqId, boolean trackPerf) {
        logger.debug("processBaggedSample('{}','{}','{}',{})", seqId, sampleId, reqId, trackPerf);
        pstep = 3;
        int ssstep = 0;
        String clinical_bucket_name = plugin.getConfig().getStringParam("clinical_bucket");
        String results_bucket_name = plugin.getConfig().getStringParam("results_bucket");
        String incoming_directory = plugin.getConfig().getStringParam("incoming_directory");
        String outgoing_directory = plugin.getConfig().getStringParam("outgoing_directory");
        String gpackage_directory = plugin.getConfig().getStringParam("gpackage_directory");
        String container_name = plugin.getConfig().getStringParam("container_name");
        try {
            sendUpdateInfoMessage(seqId, sampleId, reqId, ssstep, "Starting to process sample");
            Thread.sleep(1000);
            if (!processBaggedSampleCheckAndPrepare(seqId, sampleId, reqId, ssstep, clinical_bucket_name,
                    results_bucket_name, incoming_directory, outgoing_directory, gpackage_directory,
                    container_name)) {
                Thread.sleep(1000);
                sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                        "Failed sample processor initialization");
                pstep = 2;
                return;
            }
            ssstep++;
        } catch (InterruptedException ie) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep, "Sample processing was interrupted");
        } catch (Exception e) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                    String.format("processBaggedSample exception encountered - %s", ExceptionUtils.getStackTrace(e)));
        }
        try {
            Thread.sleep(1000);
            if (!processBaggedSampleDownloadSample(seqId, sampleId, reqId, ssstep, clinical_bucket_name,
                    incoming_directory)) {
                Thread.sleep(1000);
                sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep, "Failed to download sample file");
                pstep = 2;
                return;
            }
            ssstep++;
        } catch (InterruptedException ie) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep, "Sample processing was interrupted");
        } catch (Exception e) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                    String.format("processBaggedSample exception encountered - %s", ExceptionUtils.getStackTrace(e)));
        }
        try {
            File workDir = Paths.get(new File(incoming_directory).getCanonicalPath(), sampleId).toFile();
            File resultsDir = new File(outgoing_directory);
            File gPackageDir = new File(gpackage_directory);
            Thread.sleep(1000);
            int retCode = processBaggedSampleRunContainer(seqId, sampleId, reqId, ssstep, gPackageDir, workDir,
                    resultsDir, container_name, trackPerf);
            if (retCode != 0) {
                Thread.sleep(1000);
                if (retCode == -1)
                    sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep, "Failed to process sample");
                else {
                    switch (retCode) {
                        case 1:     // Container error encountered
                            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                                    "General Docker Error Encountered (Err: 1)");
                            break;
                        case 100:   // Script failure encountered
                            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                                    "Processing Pipeline encountered an error (Err: 100)");
                            break;
                        case 125:   // Container failed to run
                            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                                    "Container failed to run (Err: 125)");
                            break;
                        case 126:   // Container command cannot be invoked
                            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                                    "Container command failed to be invoked (Err: 126)");
                            break;
                        case 127:   // Container command cannot be found
                            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                                    "Container command could not be found (Err: 127)");
                            break;
                        case 137:   // Container was killed
                            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                                    "Container was manually stopped (Err: 137)");
                            break;
                        default:    // Other return code encountered
                            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                                    String.format("Unknown container return code (Err: %d)", retCode));
                            break;
                    }
                }
                pstep = 2;
                return;
            }
            ssstep++;
        } catch (InterruptedException ie) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep, "Sample processing was interrupted");
        } catch (Exception e) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                    String.format("processBaggedSample exception encountered - %s", ExceptionUtils.getStackTrace(e)));
        }
        try {
            File resultsDir = new File(outgoing_directory);
            Thread.sleep(1000);
            if (!processBaggedSampleUploadResults(seqId, sampleId, reqId, ssstep, resultsDir, results_bucket_name)) {
                Thread.sleep(1000);
                sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep, "Failed to upload sample results");
                pstep = 2;
                return;
            }
            ssstep++;
            Thread.sleep(1000);
            sendUpdateInfoMessage(seqId, sampleId, reqId, ssstep, "Sample processing complete");
        } catch (InterruptedException ie) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep, "Sample processing was interrupted");
        } catch (Exception e) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                    String.format("processBaggedSample exception encountered - %s", ExceptionUtils.getStackTrace(e)));
        }
        pstep = 2;
    }

    private boolean processBaggedSampleCheckAndPrepare(String seqId, String sampleId, String reqId, int ssstep,
                                                       String clinical_bucket_name, String results_bucket_name,
                                                       String incoming_directory, String outgoing_directory,
                                                       String gpackage_directory, String container_name) {
        logger.debug("processBaggedSampleCheckAndPrepare('{}','{}','{}',{})", seqId, sampleId, reqId, ssstep);
        sendUpdateInfoMessage(seqId, sampleId, reqId, ssstep, "Checking and preparing sample processor");
        try {
            ObjectEngine oe = new ObjectEngine(plugin);
            if (clinical_bucket_name == null || clinical_bucket_name.equals("")) {
                sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                        "Clinical bucket is not properly set on this processor");
                Plugin.setInactive();
                return false;
            }
            if (results_bucket_name == null || results_bucket_name.equals("")) {
                sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                        "Results bucket is not properly set on this processor");
                Plugin.setInactive();
                return false;
            }
            if (incoming_directory == null || incoming_directory.equals("")) {
                sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                        "Incoming (working) directory is not properly set on this processor");
                Plugin.setInactive();
                return false;
            }
            if (outgoing_directory == null || outgoing_directory.equals("")) {
                sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                        "Outgoing (results) directory is not properly set on this processor");
                Plugin.setInactive();
                return false;
            }
            if (gpackage_directory == null || gpackage_directory.equals("")) {
                sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                        "Genomic package directory is not properly set on this processor");
                Plugin.setInactive();
                return false;
            }
            if (container_name == null || container_name.equals("")) {
                sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                        "Docker container name is not properly set on this processor");
                Plugin.setInactive();
                return false;
            }
            logger.trace("Checking to see if clinical bucket [{}] exists", clinical_bucket_name);
            if (!oe.doesBucketExist(clinical_bucket_name)) {
                sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                        String.format("Clinical bucket [%s] does not exist for given S3 credentials", clinical_bucket_name));
                return false;
            }
            logger.trace("Checking to see if results bucket [{}] exists", results_bucket_name);
            if (!oe.doesBucketExist(results_bucket_name)) {
                sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                        String.format("Results bucket [%s] does not exist for given S3 credentials", results_bucket_name));
                return false;
            }
            File workDir = new File(incoming_directory);
            if (workDir.exists())
                if (!deleteDirectory(workDir)) {
                    sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                            String.format("Failed to remove existing work directory [%s]", incoming_directory));
                    return false;
                }
            if (!workDir.mkdirs()) {
                sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                        String.format("Failed to create work directory [%s]", incoming_directory));
                return false;
            }
            File resultsDir = new File(outgoing_directory);
            if (resultsDir.exists())
                if (!deleteDirectory(resultsDir)) {
                    sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                            String.format("Failed to remove existing results directory [%s]", outgoing_directory));
                    return false;
                }
            if (!resultsDir.mkdirs()) {
                sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                        String.format("Failed to create results directory [%s]", outgoing_directory));
                return false;
            }
            File gPackageDir = new File(gpackage_directory);
            if (!gPackageDir.exists() || !gPackageDir.isDirectory()) {
                sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                        String.format("Genomic package directory [%s] does not exist", gPackageDir.getAbsolutePath()));
                return false;
            }
            return true;
        } catch (AmazonServiceException ase) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                    String.format("Caught an AmazonServiceException, which means your request made it " +
                            "to Amazon S3, but was rejected with an error response for some reason - %s", ase.getMessage()));
            return false;
        } catch (SdkClientException ace) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                    String.format("Caught an AmazonClientException, which means the client encountered "
                            + "a serious internal problem while trying to communicate with S3, "
                            + "such as not being able to access the network - %s", ace.getMessage()));
            return false;
        } catch (Exception e) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                    String.format("Check and prepare exception encountered - %s", ExceptionUtils.getStackTrace(e)));
            return false;
        }
    }

    private boolean processBaggedSampleDownloadSample(String seqId, String sampleId, String reqId, int ssstep,
                                                      String clinical_bucket_name, String incoming_directory) {
        logger.debug("processBaggedSampleDownloadSample('{}','{}','{}',{})", seqId, sampleId, reqId, ssstep);
        sendUpdateInfoMessage(seqId, sampleId, reqId, ssstep, "Downloading sample file");
        try {
            ObjectEngine oe = new ObjectEngine(plugin);
            File workDir = Paths.get(incoming_directory, seqId).toFile();
            if (!oe.downloadBaggedDirectory(clinical_bucket_name, sampleId,
                    workDir.getAbsolutePath(), seqId, sampleId, reqId,
                    String.valueOf(ssstep))) {
                sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep, "Failed to download sample file");
                return false;
            }
            File baggedSampleFile = Paths.get(incoming_directory, sampleId + ObjectEngine.extension).toFile();
            sendUpdateInfoMessage(seqId, sampleId, reqId, ssstep,
                    String.format("Download successful, unboxing sample file [%s]", baggedSampleFile.getAbsolutePath()));
            if (!Encapsulation.unarchive(baggedSampleFile, workDir)) {
                sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                        String.format("Failed to unarchive sample file [%s]", baggedSampleFile.getAbsolutePath()));
                return false;
            }
            File unboxed = Paths.get(incoming_directory, sampleId).toFile();
            if (!unboxed.exists() || !unboxed.isDirectory()) {
                sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                        String.format("Unboxing to [%s] failed", unboxed));
                return false;
            }
            if (!baggedSampleFile.delete()) {
                sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                        String.format("Failed to delete sample archive file [%s]", baggedSampleFile.getAbsolutePath()));
            }
            sendUpdateInfoMessage(seqId, sampleId, reqId, ssstep,
                    String.format("Validating sample [%s]", unboxed.getAbsolutePath()));
            if (!Encapsulation.isBag(unboxed.getAbsolutePath())) {
                sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                        String.format("Unboxed sample [%s] does not contain BagIt data", unboxed.getAbsolutePath()));
                return false;
            }
            if (!Encapsulation.verifyBag(unboxed.getAbsolutePath(), true)) {
                sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                        String.format("Unboxed sample [%s] failed BagIt verification", unboxed.getAbsolutePath()));
                return false;
            }
            sendUpdateInfoMessage(seqId, sampleId, reqId, ssstep,
                    String.format("Restoring sample [%s]", unboxed.getAbsolutePath()));
            Encapsulation.debagify(unboxed.getAbsolutePath());
            sendUpdateInfoMessage(seqId, sampleId, reqId, ssstep,
                    String.format("Sample [%s] restored to [%s]", sampleId, unboxed));
            File commands_main = Paths.get(unboxed.getAbsolutePath(), "commands_main.sh").toFile();
            if (!commands_main.exists() || !commands_main.isFile()) {
                sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep, "Commands file is missing");
                return false;
            }
            if (!commands_main.setExecutable(true)) {
                sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep, "Failed to make commands file executable");
                return false;
            }
            return true;
        } catch (AmazonServiceException ase) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                    String.format("Caught an AmazonServiceException, which means your request made it " +
                            "to Amazon S3, but was rejected with an error response for some reason - %s", ase.getMessage()));
            return false;
        } catch (SdkClientException ace) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                    String.format("Caught an AmazonClientException, which means the client encountered "
                            + "a serious internal problem while trying to communicate with S3, "
                            + "such as not being able to access the network - %s", ace.getMessage()));
            return false;
        } catch (Exception e) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                    String.format("Sample download exception encountered - %s", ExceptionUtils.getStackTrace(e)));
            return false;
        }
    }

    private int processBaggedSampleRunContainer(String seqId, String sampleId, String reqId, int ssstep,
                                                    File gPackageDir, File workDir, File resultDir,
                                                    String container, boolean trackPerf) {
        sendUpdateInfoMessage(seqId, sampleId, reqId, ssstep, "Starting pipeline via Docker container");
        String containerName = "procsam." + sampleId.replace("/", ".");
        try {
            Process kill = Runtime.getRuntime().exec("docker kill " + containerName);
            kill.waitFor();
            Thread.sleep(500);
            Process clear = Runtime.getRuntime().exec("docker rm " + containerName);
            clear.waitFor();
        } catch (InterruptedException ie) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                    "Interrupted while clearing out existing containers");
            return -1;
        } catch (IOException ioe) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                    String.format("Failed to execute shell command(s) to clean out existing containers "
                            + "- %s", ExceptionUtils.getStackTrace(ioe)));
            return -1;
        }
        String command = String.format("docker run -t -v %s:/gpackage -v %s:/gdata/input -v %s:/gdata/output "
                        + "--name procsam.%s %s /gdata/input/commands_main.sh", gPackageDir.getAbsolutePath(),
                workDir.getAbsolutePath(), resultDir.getAbsolutePath(), sampleId.replace("/", "."),
                container);
        sendUpdateInfoMessage(seqId, sampleId, reqId, ssstep, String.format("Executing command: %s", command));

        PerfTracker pt = null;
        if (trackPerf) {
            logger.trace("Starting performance monitoring");
            pt = new PerfTracker();
            new Thread(pt).start();
        }
        StringBuilder output = new StringBuilder();
        Process p;
        try {
            p = Runtime.getRuntime().exec(command);
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
                        if (!stagePhase.equals(outputStr[3]))
                            sendUpdateInfoMessage(seqId, sampleId, reqId, ssstep,
                                    String.format("Pipeline is now in phase: %s", outputStr[3]));
                        stagePhase = outputStr[3];
                    } else if (outputStr[0].toLowerCase().equals("error"))
                        sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                                String.format("Pipeline error: %s", outputLine));
                }
                logger.debug(outputLine);
            }
            logger.trace("Waiting for Docker process to finish");
            p.waitFor();
            logger.trace("Docker exit code = {}", p.exitValue());
            MsgEvent pse;
            if (trackPerf) {
                logger.trace("Ending Performance Monitor");
                pt.isActive = false;
                logger.trace("Sending Performance Information");
                pse = plugin.genGMessage(MsgEvent.Type.INFO, "Sending Performance Information");
                pse.setParam("req_id", reqId);
                pse.setParam("seq_id", seqId);
                pse.setParam("sample_id", sampleId);
                pse.setParam("pathstage", pathStage);
                pse.setParam("ssstep", String.valueOf(ssstep));
                pse.setParam("perf_log", pt.getResults());
                plugin.sendMsgEvent(pse);
            }
            pse = plugin.genGMessage(MsgEvent.Type.INFO, "Sending Pipeline Output");
            pse.setParam("req_id", reqId);
            pse.setParam("seq_id", seqId);
            pse.setParam("sample_id", sampleId);
            pse.setParam("pathstage", pathStage);
            pse.setParam("ssstep", String.valueOf(ssstep));
            pse.setParam("output_log", output.toString());
            plugin.sendMsgEvent(pse);
            Thread.sleep(2000);
            sendUpdateInfoMessage(seqId, sampleId, reqId, ssstep, "Pipeline has completed");
            Thread.sleep(500);
            Process postClear = Runtime.getRuntime().exec("docker rm " + containerName);
            postClear.waitFor();
            return p.exitValue();
        } catch (IOException ioe) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                    String.format("Run container I/O exception encountered - %s", ExceptionUtils.getStackTrace(ioe)));
            return -1;
        } catch (InterruptedException ie) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                    String.format("Container interrupted - %s", ExceptionUtils.getStackTrace(ie)));
            return -1;
        } catch (Exception e) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                    String.format("Run container exception encountered - %s", ExceptionUtils.getStackTrace(e)));
            return -1;
        }
    }

    private boolean processBaggedSampleUploadResults(String seqId, String sampleId, String reqId, int ssstep,
                                                    File resultDir, String results_bucket_name) {
        try {
            ObjectEngine oe = new ObjectEngine(plugin);
            if (!oe.uploadBaggedDirectory(results_bucket_name,
                    Paths.get(resultDir.getAbsolutePath(), sampleId).toString(), seqId, seqId, sampleId,
                    reqId, String.valueOf(ssstep))) {
                sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep, "Failed to upload processing results");
                return true;
            }
            sendUpdateInfoMessage(seqId, sampleId, reqId, ssstep,
                    String.format("Uploaded results to [%s]", results_bucket_name));
            return true;
        } catch (Exception e) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, ssstep,
                    String.format("Results upload exception encountered - %s", ExceptionUtils.getStackTrace(e)));
            return false;
        }
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

    public void downloadBaggedResults(String seqId, String reqId) {
        logger.debug("downloadBaggedResults('{}','{}')", seqId, reqId);
        pstep = 3;
        int sstep = 0;
        String results_bucket_name = plugin.getConfig().getStringParam("results_bucket");
        String incoming_directory = plugin.getConfig().getStringParam("incoming_directory");
        String outgoing_directory = plugin.getConfig().getStringParam("outgoing_directory");
        try {
            sendUpdateInfoMessage(seqId, null, reqId, sstep, "Starting to preprocess sequence");
            Thread.sleep(1000);
            if (!downloadBaggedResultsCheckAndPrepare(seqId, reqId, sstep, results_bucket_name,
                    incoming_directory, outgoing_directory)) {
                Thread.sleep(1000);
                sendUpdateErrorMessage(seqId, null, reqId, sstep,
                        "Failed sequence preprocessor initialization");
                pstep = 2;
                return;
            }
            sstep++;
        } catch (InterruptedException ie) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep, "Results download initialization was interrupted");
        } catch (Exception e) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                    String.format("preprocessBaggedSequence exception encountered - %s", ExceptionUtils.getStackTrace(e)));
        }
        try {
            Thread.sleep(1000);
            if (!downloadBaggedResultsDownloadSequence(seqId, reqId, sstep, results_bucket_name,
                    incoming_directory)) {
                Thread.sleep(1000);
                sendUpdateErrorMessage(seqId, null, reqId, sstep, "Failed to download sequence results");
                pstep = 2;
                return;
            }
            sstep++;
            Thread.sleep(1000);
            sendUpdateInfoMessage(seqId, null, reqId, sstep, "Sequence results download complete");
        } catch (InterruptedException ie) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep, "Results download was interrupted");
        } catch (Exception e) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                    String.format("preprocessBaggedSequence exception encountered - %s",
                            ExceptionUtils.getStackTrace(e)));
        }
    }

    private boolean downloadBaggedResultsCheckAndPrepare(String seqId, String reqId, int sstep,
                                                         String results_bucket_name, String incoming_directory,
                                                         String outgoing_directory) {
        logger.debug("downloadBaggedResultsCheckAndPrepare('{}','{}',{},'{}','{}','{}')",
                seqId, reqId, sstep, results_bucket_name, incoming_directory, outgoing_directory);
        sendUpdateInfoMessage(seqId, null, reqId, sstep,
                "Checking and preparing sequence results download");
        try {
            ObjectEngine oe = new ObjectEngine(plugin);
            if (results_bucket_name == null || results_bucket_name.equals("")) {
                sendUpdateErrorMessage(seqId, null, reqId, sstep,
                        "Results bucket is not properly set on this processor");
                Plugin.setInactive();
                return false;
            }
            if (incoming_directory == null || incoming_directory.equals("")) {
                sendUpdateErrorMessage(seqId, null, reqId, sstep,
                        "Incoming (working) directory is not properly set on this processor");
                Plugin.setInactive();
                return false;
            }
            if (outgoing_directory == null || outgoing_directory.equals("")) {
                sendUpdateErrorMessage(seqId, null, reqId, sstep,
                        "Outgoing (results) directory is not properly set on this processor");
                Plugin.setInactive();
                return false;
            }
            logger.trace("Checking to see if results bucket [{}] exists", results_bucket_name);
            if (!oe.doesBucketExist(results_bucket_name)) {
                sendUpdateErrorMessage(seqId, null, reqId, sstep,
                        String.format("Research bucket [%s] does not exist for given S3 credentials",
                                results_bucket_name));
                return false;
            }
            File workDir = new File(incoming_directory);
            if (workDir.exists())
                if (!deleteDirectory(workDir)) {
                    sendUpdateErrorMessage(seqId, null, reqId, sstep,
                            String.format("Failed to remove existing work directory [%s]", incoming_directory));
                    return false;
                }
            if (!workDir.mkdirs()) {
                sendUpdateErrorMessage(seqId, null, reqId, sstep,
                        String.format("Failed to create work directory [%s]", incoming_directory));
                return false;
            }
            File resultsDir = new File(outgoing_directory);
            if (resultsDir.exists())
                if (!deleteDirectory(resultsDir)) {
                    sendUpdateErrorMessage(seqId, null, reqId, sstep,
                            String.format("Failed to remove existing results directory [%s]",
                                    resultsDir.getAbsolutePath()));
                    return false;
                }
            if (!resultsDir.mkdirs()) {
                sendUpdateErrorMessage(seqId, null, reqId, sstep,
                        String.format("Failed to create results directory [%s]", resultsDir.getAbsolutePath()));
                return false;
            }
            return true;
        } catch (AmazonServiceException ase) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                    String.format("Caught an AmazonServiceException, which means your request made it " +
                            "to Amazon S3, but was rejected with an error response for some reason - %s",
                            ase.getMessage()));
            return false;
        } catch (SdkClientException ace) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                    String.format("Caught an AmazonClientException, which means the client encountered "
                            + "a serious internal problem while trying to communicate with S3, "
                            + "such as not being able to access the network - %s", ace.getMessage()));
            return false;
        } catch (Exception e) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                    String.format("Check and prepare exception encountered - %s", ExceptionUtils.getStackTrace(e)));
            return false;
        }
    }

    private boolean downloadBaggedResultsDownloadSequence(String seqId, String reqId, int sstep,
                                                             String results_bucket_name, String incoming_directory) {
        logger.debug("downloadBaggedResultsDownloadSequence('{}','{}','{}',{})", seqId, reqId, sstep);
        sendUpdateInfoMessage(seqId, null, reqId, sstep, "Downloading sequence results");
        try {
            ObjectEngine oe = new ObjectEngine(plugin);
            File workDir = Paths.get(incoming_directory, seqId).toFile();
            Map<String, Long> samples = oe.getlistBucketContents(results_bucket_name, seqId);
            for (Map.Entry<String, Long> sample : samples.entrySet()) {
                logger.info("Sample to download: s3://{}/{} - {} bytes",
                        results_bucket_name, sample.getKey(), sample.getValue());
                String sampleId = sample.getKey();
                int idx = sampleId.lastIndexOf(".tar");
                if (idx == -1)
                    idx = sampleId.lastIndexOf(".tgz");
                if (idx > 0)
                    sampleId = sampleId.substring(0, idx);
                logger.info("SampleID: {}", sampleId);
                if (!oe.downloadBaggedDirectory(results_bucket_name, sampleId,
                        workDir.getAbsolutePath(), seqId, sampleId,
                        reqId, String.valueOf(sstep))) {
                    sendUpdateErrorMessage(seqId, sampleId, reqId, sstep, "Failed to download sequence raw file");
                    return false;
                }
                File baggedSequenceFile = Paths.get(incoming_directory, seqId + ObjectEngine.extension).toFile();
                if (!baggedSequenceFile.exists()) {
                    sendUpdateErrorMessage(seqId, sampleId, reqId, sstep, "Failed to download sequence raw file");
                    return false;
                }
                sendUpdateInfoMessage(seqId, sampleId, reqId, sstep,
                        String.format("Download successful, unboxing sample file [%s]", baggedSequenceFile.getAbsolutePath()));
                if (!Encapsulation.unarchive(baggedSequenceFile, new File(incoming_directory))) {
                    sendUpdateErrorMessage(seqId, sampleId, reqId, sstep,
                            String.format("Failed to unarchive sample file [%s]", baggedSequenceFile.getAbsolutePath()));
                    return false;
                }
                File unboxed = Paths.get(incoming_directory, seqId).toFile();
                if (!unboxed.exists() || !unboxed.isDirectory()) {
                    sendUpdateErrorMessage(seqId, sampleId, reqId, sstep,
                            String.format("Unboxing to [%s] failed", unboxed));
                    return false;
                }
                if (!baggedSequenceFile.delete()) {
                    sendUpdateErrorMessage(seqId, sampleId, reqId, sstep,
                            String.format("Failed to delete sample archive file [%s]", baggedSequenceFile.getAbsolutePath()));
                }
                sendUpdateInfoMessage(seqId, sampleId, reqId, sstep,
                        String.format("Validating sample [%s]", unboxed.getAbsolutePath()));
                if (!Encapsulation.isBag(unboxed.getAbsolutePath())) {
                    sendUpdateErrorMessage(seqId, null, reqId, sstep,
                            String.format("Unboxed sample [%s] does not contain BagIt data", unboxed.getAbsolutePath()));
                    return false;
                }
                if (!Encapsulation.verifyBag(unboxed.getAbsolutePath(), true)) {
                    sendUpdateErrorMessage(seqId, sampleId, reqId, sstep,
                            String.format("Unboxed sample [%s] failed BagIt verification", unboxed.getAbsolutePath()));
                    return false;
                }
                sendUpdateInfoMessage(seqId, sampleId, reqId, sstep,
                        String.format("Restoring sample [%s]", unboxed.getAbsolutePath()));
                Encapsulation.debagify(unboxed.getAbsolutePath());
                sendUpdateInfoMessage(seqId, sampleId, reqId, sstep,
                        String.format("Sample [%s] restored to [%s]", seqId, unboxed));
            }
            return true;
        } catch (AmazonServiceException ase) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                    String.format("Caught an AmazonServiceException, which means your request made it " +
                            "to Amazon S3, but was rejected with an error response for some reason - %s", ase.getMessage()));
            return false;
        } catch (SdkClientException ace) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                    String.format("Caught an AmazonClientException, which means the client encountered "
                            + "a serious internal problem while trying to communicate with S3, "
                            + "such as not being able to access the network - %s", ace.getMessage()));
            return false;
        } catch (Exception e) {
            sendUpdateErrorMessage(seqId, null, reqId, sstep,
                    String.format("Sequence download exception encountered - %s", ExceptionUtils.getStackTrace(e)));
            return false;
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

    private void sendUpdateInfoMessage(String seqId, String sampleId, String reqId, int stepInt, String message) {
        String step = String.valueOf(stepInt);
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

    private void sendUpdateErrorMessage(String seqId, String sampleId, String reqId, int stepInt, String message) {
        String step = String.valueOf(stepInt);
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
}



