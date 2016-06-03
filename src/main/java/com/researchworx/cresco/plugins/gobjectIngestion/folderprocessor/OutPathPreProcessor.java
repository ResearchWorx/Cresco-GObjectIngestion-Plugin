package com.researchworx.cresco.plugins.gobjectIngestion.folderprocessor;

import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import com.researchworx.cresco.plugins.gobjectIngestion.Plugin;
import com.researchworx.cresco.plugins.gobjectIngestion.objectstorage.ObjectEngine;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OutPathPreProcessor implements Runnable {
    private final String transfer_watch_file;
    private final String transfer_status_file;
    private final String incoming_directory;
    private final String bucket_name;
    private Plugin plugin;
    private CLogger logger;
    private MsgEvent me;

    public OutPathPreProcessor(Plugin plugin) {
        this.plugin = plugin;
        this.logger = new CLogger(plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Trace);

        logger.debug("OutPathPreProcessor Instantiated");
        transfer_watch_file = plugin.getConfig().getStringParam("transfer_watch_file");
        logger.debug("\"pathstage2\" --> \"transfer_watch_file\" from config [{}]", transfer_watch_file);
        transfer_status_file = plugin.getConfig().getStringParam("transfer_status_file");
        logger.debug("\"pathstage2\" --> \"transfer_status_file\" from config [{}]", transfer_status_file);
        incoming_directory = plugin.getConfig().getStringParam("incoming_directory");
        logger.debug("\"pathstage2\" --> \"incoming_directory\" from config [{}]", incoming_directory);
        bucket_name = plugin.getConfig().getStringParam("bucket");
        logger.debug("\"pathstage2\" --> \"bucket\" from config [{}]", bucket_name);
        me = plugin.genGMessage(MsgEvent.Type.INFO,"InPathPreProcessor instantiated");
        me.setParam("transfer_watch_file",transfer_watch_file);
        me.setParam("transfer_status_file", transfer_status_file);
        me.setParam("bucket_name",bucket_name);
        me.setParam("pathstage",String.valueOf(plugin.pathStage));
        me.setParam("endpoint", plugin.getConfig().getStringParam("endpoint"));
        me.setParam("pstep","1");
        plugin.sendMsgEvent(me);
    }

    @Override
    public void run() {
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
                me.setParam("pathstage",String.valueOf(plugin.pathStage));
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
                        if (!localDirs.contains(remoteDir)) {
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
                    me.setParam("pathstage",String.valueOf(plugin.pathStage));
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
                me.setParam("pathstage",String.valueOf(plugin.pathStage));
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
            me.setParam("pathstage",String.valueOf(plugin.pathStage));
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
            me.setParam("pathstage",String.valueOf(plugin.pathStage));
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
                me.setParam("pathstage",String.valueOf(plugin.pathStage));
                me.setParam("sstep","2");
                plugin.sendMsgEvent(me);
            }
        }
    }

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



