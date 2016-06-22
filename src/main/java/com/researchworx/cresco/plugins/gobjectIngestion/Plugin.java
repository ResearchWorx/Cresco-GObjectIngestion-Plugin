package com.researchworx.cresco.plugins.gobjectIngestion;

import com.google.auto.service.AutoService;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.plugin.core.CPlugin;
import com.researchworx.cresco.library.utilities.CLogger;
import com.researchworx.cresco.plugins.gobjectIngestion.folderprocessor.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

@AutoService(CPlugin.class)
public class Plugin extends CPlugin {


    //private static final Logger logger = LoggerFactory.getLogger(Plugin.class);
    private static String watchDirectoryName;

    public static ConcurrentLinkedQueue<Path> pathQueue;
    public static boolean PathProcessorActive = false;

    public int pathStage;
    public String genomicControllerRegion;
    public String genomicControllerAgent;
    public String genomicControllerPlugin;
    public static ObjectFS objectToFSp;
    public static FSObject fStoObjectp;


    public void setExecutor() {
        setExec(new Executor(this));
    }

    public void start() {
        //this.logger = new CLogger(getMsgOutQueue(), getRegion(), getAgent(), getPluginID(), CLogger.Level.Trace);
        setExec(new Executor(this));
        logger.setLogLevel(CLogger.Level.Debug);
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
        genomicControllerRegion = getConfig().getStringParam("genomic_controller_region");
        genomicControllerAgent = getConfig().getStringParam("genomic_controller_agent");
        genomicControllerPlugin = getConfig().getStringParam("genomic_controller_plugin");


        logger.debug("[pathStage] == {}", String.valueOf(pathStage));
        logger.info("Building Stage [{}]", String.valueOf(pathStage));
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
                Thread wdt = new Thread(wd);
                wdt.start();
            }
            catch (Exception ex) {
                logger.error(ex.getMessage());
            }

        }
    }

    public String getSysInfoPlugin() {

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
                MsgEvent me = getSysInfo();
                if(me != null) {
                    //logger.info(me.getParams().toString());
                    Iterator it = me.getParams().entrySet().iterator();
                    while (it.hasNext()) {
                        Map.Entry pairs = (Map.Entry) it.next();
                        logger.info(pairs.getKey() + " = " + pairs.getValue());
                        //String plugin = pairs.getKey().toString();
                    }
                    //cpu-per-cpu-load = CPU Load per processor: 1.0% 12.0% 8.0% 7.9% 0.0% 0.0% 0.0% 0.0%
                    //cpu-core-count = 8
                    String sCoreCount = me.getParam("cpu-core-count");
                    int coreCount = Integer.parseInt(sCoreCount);
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

                    String header = "cpu-idle-load,cpu-user-load,cpu-nice-load,cpu-sys-load,cpu-core-count,cpu-core-load,load-sane,memory-total,memory-available,memory-used\n";
                    String output = sCpuIdleLoad + "," + sCpuUserLoad + "," + sCpuNiceLoad + "," + sCpuSysLoad + "," + sCoreCount + "," + sCputPerLoadGrp + "," + String.valueOf(loadIsSane) + "," + sMemoryTotal + "," + sMemoryAvailable + "," + sMemoryUsed + "\n";


                    String logPath = getConfig().getStringParam("perflogpath");
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
            catch(Exception ex) {
                logger.error("Static runner failure : " + ex.getMessage());
            }
        }
    }

    public MsgEvent genAgentMessage() {
        MsgEvent me = null;
        try {
            logger.debug("Generated Agent Message");
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
            logger.debug("Generated Genomics Message");
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
        MsgEvent me = genGMessage(MsgEvent.Type.INFO, "Shutdown");
        me.setParam("pathstage", String.valueOf(pathStage));
        me.setParam("pstep","0");
        sendMsgEvent(me);
    }
}
