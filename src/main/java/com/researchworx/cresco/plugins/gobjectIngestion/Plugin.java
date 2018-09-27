package com.researchworx.cresco.plugins.gobjectIngestion;

import com.google.auto.service.AutoService;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.plugin.core.CPlugin;
import com.researchworx.cresco.plugins.gobjectIngestion.folderprocessor.FSObject;
import com.researchworx.cresco.plugins.gobjectIngestion.folderprocessor.ObjectFS;
import com.researchworx.cresco.plugins.gobjectIngestion.folderprocessor.WatchDirectory;
import com.researchworx.cresco.plugins.gobjectIngestion.objectstorage.Encapsulation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentLinkedQueue;

@AutoService(CPlugin.class)
public class Plugin extends CPlugin {
    private static String watchDirectoryName;

    public static ConcurrentLinkedQueue<Path> pathQueue;
    public static boolean PathProcessorActive = false;

    public int pathStage;

    private String genomicControllerRegion;
    private String genomicControllerAgent;
    private String genomicControllerPlugin;

    static ObjectFS objectToFSp;
    static FSObject fStoObjectp;

    public static boolean processorIsActive() {
        return PathProcessorActive;
    }

    public static void setActive() {
        PathProcessorActive = true;
    }

    public static void setInactive() {
        PathProcessorActive = false;
    }

    public void setExecutor() {
        setExec(new Executor(this));
    }

    public void start() {
        /*try {
            URL instanceIDURL = new URL("http://169.254.169.254/latest/meta-data/instance-id");
            HttpURLConnection conn = (HttpURLConnection) instanceIDURL.openConnection();
            conn.setRequestMethod("GET");
            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String instanceID = in.readLine();
            logger.info("Instance ID: {}", (instanceID != null) ? instanceID : "NULL");
        } catch (ProtocolException e) {
            logger.error("Protocol exception when getting instance ID: {}", e.getMessage());
        } catch (IOException e) {
            logger.error("I/O exception when getting instance ID: {}", e.getMessage());
        }*/


        setExec(new Executor(this));
        //logger.setLogLevel(CLogger.Level.Debug);
        logger.trace("Building new ConcurrentLinkedQueue");
        pathQueue = new ConcurrentLinkedQueue<>();

        logger.trace("Provisioning uninstantiated ppThread with null");
        Thread ppThread = null;

        logger.trace("Grabbing [pathstage] from config");
        if(getConfig().getStringParam("pathstage") == null) {
            logger.error("Pathstage config not found exiting!");
            System.exit(0);
        }
        pathStage = getConfig().getIntegerParam("pathstage");
        genomicControllerRegion = getConfig().getStringParam("genomic_controller_region",getRegion());
        genomicControllerAgent = getConfig().getStringParam("genomic_controller_agent",getAgent());
        genomicControllerPlugin = getConfig().getStringParam("genomic_controller_plugin");
        Encapsulation.setLogger(this);

        logger.debug("[pathStage] == {}", pathStage);
        logger.info("Building Stage [{}]", pathStage);
        switch (pathStage) {
            case 1:
                logger.trace("Grabbing [pathstage1 --> watchdirectory] string and setting to [watchDirectoryName]");
                watchDirectoryName = getConfig().getStringParam("watchdirectory");
                logger.debug("Generating new [FStoObject] runnable");
                //InPathPreProcessor ippp = new InPathPreProcessor(this);
                //FSObject fStoObjectpp = new FSObject(this);
                fStoObjectp = new FSObject(this);
                logger.trace("Building Thread around new [FStoObject] runnable");
                //ppThread = new Thread(ippp);
                ppThread = new Thread(fStoObjectp);
                break;
            case 2:
                logger.debug("Generating new [OutPathPreProcessor] runnable");
                //OutPathPreProcessor oppp = new OutPathPreProcessor(this);
                objectToFSp = new ObjectFS(this);
                logger.trace("Building ppThread around new [OutPathPreProcessor] runnable");
                ppThread = new Thread(objectToFSp);
                break;
            case 3:
                logger.info("Grabbing [pathstage3 --> watchdirectory] string and setting to [watchDirectoryName]");
                watchDirectoryName = getConfig().getStringParam("watchdirectory");
                logger.info("WatchDirectoryName=" + watchDirectoryName);
                logger.info("Generating new [InPathProcessor] runnable");
                //InPathProcessor pp = new InPathProcessor(this);
                fStoObjectp = new FSObject(this);
                logger.info("Building ppThread around new [InPathProcessor] runnable");
                ppThread = new Thread(fStoObjectp);
                break;
            case 4:
                logger.debug("Generating new [OutPathProcessor] runnable");
                //OutPathProcessor opp = new OutPathProcessor(this);
                objectToFSp = new ObjectFS(this);
                if((config.getStringParam("static_process_indir") != null) && (config.getStringParam("static_process_outdir") != null)) {
                    //objectToFSp.executeCommand(config.getStringParam("static_process_indir"),config.getStringParam("static_process_outdir"), true);
                    StaticRunner sr = new StaticRunner();
                    Thread srt = new Thread(sr);
                    srt.start();
                }
                logger.trace("Building pThread around new [OutPathProcessor] runnable");
                ppThread = new Thread(objectToFSp);

                break;
            case 5:
                logger.debug("Generating new [OutPathDeliverer] runnable");
                objectToFSp = new ObjectFS(this);
                logger.trace("Building ppThread around new [OutPathDeliverer] runnable");
                ppThread = new Thread(objectToFSp);
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
        logger.info("Starting Stage [{}] Object Ingestion", pathStage);
        ppThread.start();

        logger.trace("Checking [watchDirectoryName] for null");
        if (watchDirectoryName != null) {
            logger.trace("Grabbing path for [watchDirectoryName]");
            Path dir = Paths.get(watchDirectoryName);
            logger.trace("Instantiating new [WatchDirectory] from [watchDirectoryName] path");
            WatchDirectory wd;
            try {
                wd = new WatchDirectory(dir, true, this);
                Thread wdt = new Thread(wd);
                wdt.start();
            }
            catch (Exception ex) {
                logger.error(ex.getMessage());
            }

        }
    }

    private String getSysInfoPlugin() {
        String sysPlugin = null;
        try {

            MsgEvent me = genAgentMessage();
            me.setParam("cmd", "getactiveplugins");
            MsgEvent re = sendRPC(me);
            String[] activePluginList = re.getParam("activepluginlist").split(",");
            for (String pluginName : activePluginList) {
                String[] pstr = pluginName.split("=");
                if (pstr[0].equals("cresco-sysinfo-plugin")) {
                    sysPlugin = pstr[1];
                }
            }
        } catch (Exception ex) {
            logger.error("getSysInfoPlugin() " + ex.getMessage());
        }
        return sysPlugin;
    }

    public MsgEvent getSysInfo() {

        MsgEvent rm = null;
        try {
            String sysPlugin = getSysInfoPlugin();
            if(sysPlugin != null) {
                MsgEvent me = genAgentMessage();
                me.setParam("dst_plugin",sysPlugin);
                rm = sendRPC(me);
            }
            else {
                logger.error("getSysInfo no sysinfo plugin exist on local agent!");
            }
        } catch (Exception ex) {
            logger.error("getSysInfo() " + ex.getMessage());
        }
        return rm;
    }

    private class StaticRunner extends Thread {

        public void run(){
            try {
                Thread.sleep(2000);
                System.out.println("StaticRunner running");
                String inDir = config.getStringParam("static_process_indir");
                String outDir = config.getStringParam("static_process_outdir");

                //objectToFSp.run_test();
                objectToFSp.executeCommand(inDir,outDir,true);

            }
            catch(Exception ex) {
                logger.error("Static runner failure : " + ex.getMessage());
            }
        }
    }

    private MsgEvent genAgentMessage() {
        MsgEvent me = null;
        try {
            logger.trace("Generated Agent Message");
            //MsgEvent.Type
            me = new MsgEvent(MsgEvent.Type.EXEC,getRegion(),getAgent(),getPluginID(),"generated agent message");
            me.setParam("src_region",getRegion());
            me.setParam("src_agent",getAgent());
            me.setParam("src_plugin",getPluginID());
            me.setParam("dst_region",getRegion());
            me.setParam("dst_agent", getAgent());
            logger.trace(me.getParams().toString());
        }
        catch(Exception ex) {
            logger.error(ex.getMessage());
        }
        return me;
    }

    public MsgEvent genGMessage(MsgEvent.Type met, String msgBody) {
        MsgEvent me = null;
        try {
            logger.trace("Generated Genomics Message");
            //MsgEvent.Type
            me = new MsgEvent(MsgEvent.Type.EXEC,getRegion(),getAgent(),getPluginID(),msgBody);
            me.setParam("src_region",getRegion());
            me.setParam("src_agent",getAgent());
            me.setParam("src_plugin",getPluginID());
            me.setParam("dst_region",genomicControllerRegion);
            me.setParam("dst_agent", genomicControllerAgent);
            me.setParam("dst_plugin", genomicControllerPlugin);
            me.setParam("gmsg_type",met.name());
            logger.trace(me.getParams().toString());
        }
        catch(Exception ex) {
            logger.error(ex.getMessage());
        }
        return me;
    }

    @Override
    public void cleanUp() {
        setInactive();
        MsgEvent me = genGMessage(MsgEvent.Type.INFO, "Shutdown");
        me.setParam("pathstage", String.valueOf(pathStage));
        me.setParam("pstep","0");
        sendMsgEvent(me);
    }
}
