package com.researchworx.cresco.plugins.gobjectIngestion.folderprocessor;

import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import com.researchworx.cresco.plugins.gobjectIngestion.Plugin;
import com.researchworx.cresco.plugins.gobjectIngestion.objectstorage.ObjectEngine;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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
    private MsgEvent me;
    private String pathStage;
    private int pstep;
    public static String stagePhase;

    public ObjectFS(Plugin plugin) {
        this.stagePhase = "uninit";
        this.pstep = 1;
        this.plugin = plugin;
        this.logger = new CLogger(ObjectFS.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Debug);
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

        me = plugin.genGMessage(MsgEvent.Type.INFO,"InPathPreProcessor instantiated");
        me.setParam("transfer_watch_file",transfer_watch_file);
        me.setParam("transfer_status_file", transfer_status_file);
        me.setParam("bucket_name",bucket_name);
        me.setParam("pathstage",pathStage);
        me.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
        me.setParam("pstep",String.valueOf(pstep));
        plugin.sendMsgEvent(me);


    }

    @Override
    public void run() {
        try {
            pstep = 2;
            logger.trace("Setting [PathProcessorActive] to true");
            plugin.PathProcessorActive = true;
            ObjectEngine oe = new ObjectEngine(plugin);
            logger.trace("Entering while-loop");
            while (plugin.PathProcessorActive) {
                me = plugin.genGMessage(MsgEvent.Type.INFO, "Idle");
                me.setParam("transfer_watch_file", transfer_watch_file);
                me.setParam("transfer_status_file", transfer_status_file);
                me.setParam("bucket_name", bucket_name);
                me.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                me.setParam("pathstage", pathStage);
                me.setParam("pstep",String.valueOf(pstep));
                plugin.sendMsgEvent(me);
                Thread.sleep(plugin.getConfig().getIntegerParam("scan_interval",5000));
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
            me.setParam("pstep",String.valueOf(pstep));
            plugin.sendMsgEvent(me);
        }
    }

    public void processSequence(String seqId, String reqId) {
        MsgEvent pse = null;
        try {
            pstep = 3;
            logger.debug("Call to processSequence seq_id: " + seqId, ", req_id: " + reqId);
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
            pse.setParam("sstep", "1");
            plugin.sendMsgEvent(pse);

            oe.downloadDirectory(bucket_name, seqId, incoming_directory, seqId, null);

            List<String> filterList = new ArrayList<>();
            logger.trace("Add [transfer_status_file] to [filterList]");
            filterList.add(transfer_status_file);
            String inDir = incoming_directory;
            if (!inDir.endsWith("/")) {
                inDir = inDir + "/";
            }
            logger.debug("[inDir = {}]", inDir);
            oe = new ObjectEngine(plugin);
            if (oe.isSyncDir(bucket_name, seqId, inDir, filterList)) {
                logger.debug("Directory Sycned [inDir = {}]", inDir);
                Map<String, String> md5map = oe.getDirMD5(inDir, filterList);
                logger.trace("Set MD5 hash");
                setTransferFileMD5(inDir + transfer_status_file, md5map);
                pse = plugin.genGMessage(MsgEvent.Type.INFO, "Directory Transfered");
                pse.setParam("indir", inDir);
                pse.setParam("seq_id", seqId);
                pse.setParam("req_id", reqId);
                pse.setParam("transfer_status_file", transfer_status_file);
                pse.setParam("bucket_name", bucket_name);
                pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                pse.setParam("pathstage", pathStage);
                pse.setParam("sstep", "2");
                plugin.sendMsgEvent(pse);
            }
        }
        catch(Exception ex) {
            logger.error("run {}", ex.getMessage());
            pse = plugin.genGMessage(MsgEvent.Type.ERROR,"Error Path Run");
            pse.setParam("seq_id", seqId);
            pse.setParam("req_id", reqId);
            pse.setParam("transfer_watch_file",transfer_watch_file);
            pse.setParam("transfer_status_file", transfer_status_file);
            pse.setParam("bucket_name",bucket_name);
            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
            pse.setParam("pathstage",pathStage);
            pse.setParam("error_message",ex.getMessage());
            pse.setParam("sstep","1");
            plugin.sendMsgEvent(pse);
        }
        pstep = 2;
    }

    private boolean deleteDirectory(File path) {
        if( path.exists() ) {
            File[] files = path.listFiles();
            for(int i=0; i<files.length; i++) {
                if(files[i].isDirectory()) {
                    deleteDirectory(files[i]);
                }
                else {
                    files[i].delete();
                }
            }
        }
        return( path.delete() );
    }

    public void run_test() {
        PerfTracker pt = new PerfTracker();
        Thread ptt = new Thread(pt);
        ptt.start();
    }

    private class PerfTracker extends Thread {

        private boolean isActive = false;
        public void run(){
            try {
                isActive = true;
                System.out.println("PerfTracker running");
                Long perfRate = plugin.getConfig().getLongParam("perfrate",5000L);
                while(isActive) {
                    logPerf();
                    Thread.sleep(perfRate);

                }

            }
            catch(Exception ex) {
                logger.error("Static runner failure : " + ex.getMessage());
            }
        }

        private void logPerf() {

            MsgEvent me = plugin.getSysInfo();
            if(me != null) {

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
                cpuPerLoad = cpuPerLoad.replace("%","");
                String[] perCpu = cpuPerLoad.split(" ");
                String sCputPerLoadGrp = "";
                for(String cpu : perCpu) {
                    //logger.info(cpu);
                    sCputPerLoadGrp += cpu + ":";
                }
                sCputPerLoadGrp = sCputPerLoadGrp.substring(0,sCputPerLoadGrp.length() -1);

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
                float cpuSysLoad  = Float.parseFloat(sCpuSysLoad);
                float cpuTotalLoad = cpuIdleLoad + cpuUserLoad + cpuNiceLoad + cpuSysLoad;

                String smemoryUsed = String.valueOf(memoryUsed/1024/1024);
                //String sCpuTotalLoad = String.valueOf(cpuTotalLoad);
                boolean loadIsSane = false;
                if(cpuTotalLoad == 100.0) {
                    loadIsSane = true;
                }

                //logger.info("MEM USED = " + smemoryUsed + " sTotalLoad = " + sCpuTotalLoad + " isSane = " + loadIsSane);

                String header = "ts,cpu-idle-load,cpu-user-load,cpu-nice-load,cpu-sys-load,cpu-core-count-physical,cpu-core-count-logical,cpu-core-load,load-sane,memory-total,memory-available,memory-used,process-phase\n";
                String output = System.currentTimeMillis() + "," + sCpuIdleLoad + "," + sCpuUserLoad + "," + sCpuNiceLoad + "," + sCpuSysLoad + "," + sCoreCountp + "," + sCoreCountl + "," + sCputPerLoadGrp + "," + String.valueOf(loadIsSane) + "," + sMemoryTotal + "," + sMemoryAvailable + "," + sMemoryUsed + "," + ObjectFS.stagePhase + "\n";


                String logPath = plugin.getConfig().getStringParam("perflogpath");
                if(logPath != null) {
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
                }

            }
            else {
                logger.error("me = null");
            }
        }
    }

    public void executeCommand(String inDir, String outDir, boolean trackPerf) {
        pstep = 3;
        //start perf mon
        PerfTracker pt = null;
        if(trackPerf) {
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
                for(int i = 0; i<outputStr.length; i++) {
                    outputStr[i] = outputStr[i].trim();
                }


                if((outputStr.length == 5) && ((outputLine.toLowerCase().startsWith("info")) || (outputLine.toLowerCase().startsWith("error")))) {
                    Calendar cal = Calendar.getInstance();
                    SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.US);
                    cal.setTime(sdf.parse(outputStr[1].trim()));// all done

                    long logdiff = (cal.getTimeInMillis() - difftime);
                    difftime = cal.getTimeInMillis();

                    if(outputStr[0].toLowerCase().equals("info")) {
                        //logger.info("Log diff = " + logdiff + " : " +  outputStr[2] + " : " + outputStr[3] + " : " + outputStr[4]);
                        ObjectFS.stagePhase = outputStr[3];
                    }
                    else if (outputStr[0].toLowerCase().equals("error")) {
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
            if(trackPerf) {
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
        pstep = 3;
        String results_bucket_name = plugin.getConfig().getStringParam("results_bucket");
        if (results_bucket_name == null || results_bucket_name.equals("")) {
            plugin.PathProcessorActive = false;
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
        }


        MsgEvent pse;
        int ssstep = 1;
        String workDirName = null;
        try {
            workDirName = incoming_directory; //create random tmp location
            workDirName = workDirName.replace("//","/");
            if(!workDirName.endsWith("/")) {
                workDirName += "/";
            }
            File workDir = new File(workDirName);
            if (workDir.exists()) {
                deleteDirectory(workDir);
            }
            workDir.mkdir();


            String remoteDir = seqId + "/" + sampleId + "/";

            logger.debug("Call to processSample seq_id: {}, sample_id: {}, req_id: {}", seqId, sampleId, reqId);
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
                Map<String, String> md5map = oe.getDirMD5(workDirName, filterList);
                logger.trace("Set MD5 hash");
                setTransferFileMD5(workDirName + transfer_status_file, md5map);
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
        }
        catch(Exception ex) {
            logger.error("run {}", ex.getMessage());
            pse = plugin.genGMessage(MsgEvent.Type.ERROR,"Error Path Run");
            pse.setParam("req_id", reqId);
            pse.setParam("seq_id", seqId);
            pse.setParam("sample_id", sampleId);
            pse.setParam("transfer_watch_file",transfer_watch_file);
            pse.setParam("transfer_status_file", transfer_status_file);
            pse.setParam("bucket_name",bucket_name);
            pse.setParam("results_bucket_name", results_bucket_name);
            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
            pse.setParam("pathstage",pathStage);
            pse.setParam("error_message",ex.getMessage());
            pse.setParam("ssstep",String.valueOf(ssstep));
            plugin.sendMsgEvent(pse);
        }

        //if is makes it through process the seq
        if(ssstep == 3) {
            logger.trace("seq_id=" + seqId + " sample_id=" + sampleId);

            try {
                //start perf mon
                PerfTracker pt = null;
                if (trackPerf) {
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
                resultDirName = resultDirName + seqId + "/" + sampleId + "/";
                File resultDir = new File(resultDirName);
                if (resultDir.exists()) {
                    deleteDirectory(resultDir);
                }
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

                String command = "docker run -t -v /home/gpackage:/gpackage -v " + workDirName + ":/gdata/input -v " + resultDirName + ":/gdata/output  intrepo.uky.edu:5000/gbase /gdata/input/commands_main.sh";

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
                                ObjectFS.stagePhase = outputStr[3];
                            } else if (outputStr[0].toLowerCase().equals("error")) {
                                logger.error("Pipeline Error : " + outputLine);
                                pse = plugin.genGMessage(MsgEvent.Type.ERROR, "Pipeline now in phase " + outputStr[3]);
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

                    p.waitFor();
                    if (trackPerf) {
                        pt.isActive = false;
                    }
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
                pse.setParam("output_log", output.toString());
                plugin.sendMsgEvent(pse);

                ObjectEngine oe = new ObjectEngine(plugin);

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

                oe.uploadDirectory(results_bucket_name, resultDirName, seqId + "/" + sampleId + "/");

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
                if (oe.isSyncDir(bucket_name, seqId + "/" + sampleId + "/", resultDirName, filterList)) {
                    ssstep = 7;
                    logger.debug("Results Directory Sycned [inDir = {}]", workDirName);
                    Map<String, String> md5map = oe.getDirMD5(workDirName, filterList);
                    logger.trace("Set MD5 hash");
                    setTransferFileMD5(workDirName + transfer_status_file, md5map);
                    pse = plugin.genGMessage(MsgEvent.Type.INFO, "Results Directory Transferred");
                    pse.setParam("indir", workDirName);
                    pse.setParam("req_id", reqId);
                    pse.setParam("seq_id", seqId);
                    pse.setParam("sample_id", sampleId);
                    pse.setParam("transfer_status_file", transfer_status_file);
                    pse.setParam("bucket_name", bucket_name);
                    pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                    pse.setParam("pathstage", pathStage);
                    pse.setParam("ssstep", String.valueOf(ssstep));
                    plugin.sendMsgEvent(pse);
                }

/*
            UUID id = UUID.randomUUID(); //create random tmp location
            String tmpInput = incoming_directory + id.toString();
            String tmpOutput = outgoing_directory + "/" + id.toString();
            String tmpRemoteOutput = remoteDir + "/" + subDir + "/" + "primary";
            tmpRemoteOutput = tmpRemoteOutput.replace("//","/");
            File tmpOutputdir = new File(tmpOutput);
            if (commands_main.exists()) {
                deleteDirectory(tmpOutputdir);
            }
            tmpOutputdir.mkdir();

            logger.trace("Creating tmp output location : " + tmpOutput);
            logger.info("Launching processing container:");
            logger.info("Input Location: " + tmpInput);
            logger.info("Output Location: " + tmpOutput);
            logger.info("Remote Output Location: " + tmpRemoteOutput);

            //process data
            //String command = "docker run -t -v /home/gpackage:/gpackage -v /home/gdata/input/160427_D00765_0033_AHKM2CBCXX/Sample3:/gdata/input -v /home/gdata/output/f8de921b-fdfa-4365-bf7d-39817b9d1883:/gdata/output  intrepo.uky.edu:5000/gbase /gdata/input/commands_main.sh";
            //String command = "docker run -t -v /home/gpackage:/gpackage -v " + tmpInput + ":/gdata/input -v " + tmpOutput + ":/gdata/output  intrepo.uky.edu:5000/gbase /gdata/input/commands_main.sh";
            String command = "dir";
            logger.info("Docker exec command: " + command);
            executeCommand(command);
            String content = "Hello File!";
            String path = tmpOutput + "/testfile";
            try {
                Files.write(Paths.get(path), content.getBytes(), StandardOpenOption.CREATE);
            }
            catch (Exception ex) {
                logger.error(ex.getMessage());
            }
            //transfer data
            logger.info("Transfering " + tmpOutput + " to " + bucket_name + ":" + tmpRemoteOutput);
            ObjectEngine oe = new ObjectEngine(plugin);
            if (oe.uploadDirectory(bucket_name, tmpOutput, tmpRemoteOutput)) {
                //cleanup
                logger.trace("Removing tmp output location : " + tmpOutput);
                deleteDirectory(tmpOutputdir);
            } else {
                logger.error("Skipping! : commands_main.sh and config_files not found in subdirectory " + dir + "/" + subDir);
            }
*/
            } catch (Exception e) {
                logger.error("processSample {}", e.getMessage());
                pse = plugin.genGMessage(MsgEvent.Type.ERROR,"Error Path Run");
                pse.setParam("req_id", reqId);
                pse.setParam("seq_id", seqId);
                pse.setParam("sample_id", sampleId);
                pse.setParam("transfer_watch_file",transfer_watch_file);
                pse.setParam("transfer_status_file", transfer_status_file);
                pse.setParam("bucket_name",bucket_name);
                pse.setParam("results_bucket_name", results_bucket_name);
                pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                pse.setParam("pathstage",pathStage);
                pse.setParam("error_message",e.getMessage());
                pse.setParam("ssstep", String.valueOf(ssstep));
                plugin.sendMsgEvent(pse);
            }
        }
        pstep = 2;
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

    private List<String> getWalkPath(String path) {
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
    }
}



