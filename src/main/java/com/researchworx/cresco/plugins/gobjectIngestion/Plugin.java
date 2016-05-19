package com.researchworx.cresco.plugins.gobjectIngestion;

import com.google.auto.service.AutoService;
import com.researchworx.cresco.library.plugin.core.CPlugin;
import com.researchworx.cresco.plugins.gobjectIngestion.folderprocessor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentLinkedQueue;

@AutoService(CPlugin.class)
public class Plugin extends CPlugin {


    private static final Logger logger = LoggerFactory.getLogger(Plugin.class);
    private static String watchDirectoryName;

    public static ConcurrentLinkedQueue<Path> pathQueue;
    public static boolean PathProcessorActive = false;

    public void setExecutor() {
        setExec(new Executor(this));
    }

    public void start() {
        logger.trace("Building new ConcurrentLinkedQueue");
        pathQueue = new ConcurrentLinkedQueue<>();

        logger.trace("Provisioning uninstantiated ppThread with null");
        Thread ppThread = null;

        logger.trace("Grabbing [pathstage] from config");
        if(getConfig().getStringParam("pathstage") == null) {
            logger.error("Pathstage config not found exiting!");
            System.exit(0);
        }
        int pathStage = getConfig().getIntegerParam("pathstage");
        logger.debug("[pathStage] == {}", pathStage);
        logger.info("Building Stage [{}]", pathStage);
        switch (pathStage) {
            case 1:
                logger.trace("Grabbing [pathstage1 --> watchdirectory] string and setting to [watchDirectoryName]");
                watchDirectoryName = getConfig().getStringParam("watchdirectory");
                logger.debug("Generating new [InPathPreProcessor] runnable");
                InPathPreProcessor ippp = new InPathPreProcessor(this);
                logger.trace("Building ppThread around new [InPathPreProcessor] runnable");
                ppThread = new Thread(ippp);
                break;
            case 2:
                logger.debug("Generating new [OutPathPreProcessor] runnable");
                OutPathPreProcessor oppp = new OutPathPreProcessor(this);
                logger.trace("Building ppThread around new [OutPathPreProcessor] runnable");
                ppThread = new Thread(oppp);
                break;
            case 3:
                logger.info("Grabbing [pathstage3 --> watchdirectory] string and setting to [watchDirectoryName]");
                watchDirectoryName = getConfig().getStringParam("watchdirectory");
                logger.info("WatchDirectoryName=" + watchDirectoryName);
                logger.info("Generating new [InPathProcessor] runnable");
                InPathProcessor pp = new InPathProcessor(this);
                logger.info("Building ppThread around new [InPathProcessor] runnable");
                ppThread = new Thread(pp);
                break;
            case 4:
                logger.debug("Generating new [OutPathProcessor] runnable");
                OutPathProcessor opp = new OutPathProcessor(this);
                logger.trace("Building pThread around new [OutPathProcessor] runnable");
                ppThread = new Thread(opp);
                break;
            case 5:
                //String command = "docker run -t -v /home/gpackage:/gpackage -v /home/gdata/input/160427_D00765_0033_AHKM2CBCXX/Sample3:/gdata/input -v /home/gdata/output/f8de921b-fdfa-4365-bf7d-39817b9d1883:/gdata/output  intrepo.uky.edu:5000/gbase /gdata/input/commands_main.sh";
                //System.out.println(command);
                //executeCommand(command);
                //test();
                break;
            default:
                logger.trace("Encountered default switch path");
                break;
        }
        logger.trace("Checking to ensure that ppThread has been instantiated");
        if (ppThread == null) {
            logger.error("PreProcessing Thread failed to generate, exiting...");
            return;
        }
        logger.info("Starting Stage [{}] Object Ingestion");
        ppThread.start();

        logger.trace("Checking [watchDirectoryName] for null");
        if (watchDirectoryName != null) {
            logger.trace("Grabbing path for [watchDirectoryName]");
            Path dir = Paths.get(watchDirectoryName);
            logger.trace("Instantiating new [WatchDirectory] from [watchDirectoryName] path");
            WatchDirectory wd = null;
            try {
                wd = new WatchDirectory(dir, true, this);
            }
            catch (Exception ex) {
                logger.error(ex.getMessage());
            }

            logger.trace("Starting Directory Watcher");
            wd.processEvents();
        }
    }

    @Override
    public void cleanUp() {
        /*
         *  Insert your shutdown / clean up code here
         */
    }
}
