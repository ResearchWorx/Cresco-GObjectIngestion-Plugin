package com.researchworx.cresco.plugins.gobjectIngestion.folderprocessor;

import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import com.researchworx.cresco.plugins.gobjectIngestion.Plugin;
import com.researchworx.cresco.plugins.gobjectIngestion.objectstorage.ObjectEngine;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ObjectFS implements Runnable {

    private final String transfer_watch_file;
    private final String transfer_status_file;

    private String bucket_name;
    private String incoming_directory;
    private Plugin plugin;
    private CLogger logger;
    private MsgEvent me;
    private String pathStage;
    private int pstep;

    public ObjectFS(Plugin plugin) {
        this.pstep = 1;
        this.plugin = plugin;
        this.logger = new CLogger(plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Trace);
        this.pathStage = String.valueOf(plugin.pathStage);
        logger.debug("OutPathPreProcessor Instantiated");
        incoming_directory = plugin.getConfig().getStringParam("incoming_directory");
        logger.debug("\"pathstage" + pathStage + "\" --> \"incoming_directory\" from config [{}]", incoming_directory);
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

    public void processSequence(String seqId) {
        MsgEvent pse = null;
        try {
            pstep = 3;
            logger.debug("Call to processSequence seq_id: " + seqId);
            ObjectEngine oe = new ObjectEngine(plugin);


            pse = plugin.genGMessage(MsgEvent.Type.INFO, "Directory Transfering");
            //me.setParam("inDir", remoteDir);
            //me.setParam("outDir", incoming_directory);
            pse.setParam("seq_id", seqId);
            pse.setParam("transfer_status_file", transfer_status_file);
            pse.setParam("bucket_name", bucket_name);
            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
            pse.setParam("pathstage", pathStage);
            pse.setParam("sstep", "1");
            plugin.sendMsgEvent(pse);

            oe.downloadDirectory(bucket_name, seqId, incoming_directory);

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
            me.setParam("transfer_watch_file",transfer_watch_file);
            me.setParam("transfer_status_file", transfer_status_file);
            me.setParam("bucket_name",bucket_name);
            me.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
            pse.setParam("pathstage",pathStage);
            pse.setParam("error_message",ex.getMessage());
            pse.setParam("sstep","1");
            plugin.sendMsgEvent(me);
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

    public void processSample(String seqId, String sampleId) {
        MsgEvent pse = null;
        int SStep = 1;

        try {
            String workDirName = incoming_directory + "/" + UUID.randomUUID().toString(); //create random tmp location
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

            pstep = 3;
            logger.debug("Call to processSequence seq_id: " + seqId);
            ObjectEngine oe = new ObjectEngine(plugin);


            pse = plugin.genGMessage(MsgEvent.Type.INFO, "Directory Transfering");
            //me.setParam("inDir", remoteDir);
            //me.setParam("outDir", incoming_directory);
            pse.setParam("seq_id", seqId);
            pse.setParam("transfer_status_file", transfer_status_file);
            pse.setParam("bucket_name", bucket_name);
            pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
            pse.setParam("pathstage", pathStage);
            pse.setParam("sstep", String.valueOf(SStep));
            plugin.sendMsgEvent(pse);

            oe.downloadDirectory(bucket_name, remoteDir, workDirName);

            workDirName += "/" + remoteDir;


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
                logger.debug("Directory Sycned [inDir = {}]", workDirName);
                Map<String, String> md5map = oe.getDirMD5(workDirName, filterList);
                logger.trace("Set MD5 hash");
                //setTransferFileMD5(workDirName + transfer_status_file, md5map);
                pse = plugin.genGMessage(MsgEvent.Type.INFO, "Directory Transfered");
                pse.setParam("indir", workDirName);
                pse.setParam("seq_id", seqId);
                pse.setParam("transfer_status_file", transfer_status_file);
                pse.setParam("bucket_name", bucket_name);
                pse.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
                pse.setParam("pathstage", pathStage);
                pse.setParam("sstep", String.valueOf(SStep));
                plugin.sendMsgEvent(pse);
                SStep = 4;
            }
        }
        catch(Exception ex) {
            logger.error("run {}", ex.getMessage());
            pse = plugin.genGMessage(MsgEvent.Type.ERROR,"Error Path Run");
            me.setParam("transfer_watch_file",transfer_watch_file);
            me.setParam("transfer_status_file", transfer_status_file);
            me.setParam("bucket_name",bucket_name);
            me.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
            pse.setParam("pathstage",pathStage);
            pse.setParam("error_message",ex.getMessage());
            pse.setParam("sstep",String.valueOf(SStep));
            plugin.sendMsgEvent(me);
        }
        //if is makes it through process the seq
        if(SStep == 4) {
            logger.trace("seq_id=" + seqId + " sample_id=" + sampleId);
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
        }

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



