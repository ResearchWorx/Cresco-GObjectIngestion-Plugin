package com.researchworx.cresco.plugins.gobjectIngestion.folderprocessor;

import com.researchworx.cresco.plugins.gobjectIngestion.Plugin;
import com.researchworx.cresco.plugins.gobjectIngestion.objectstorage.ObjectEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.*;

public class OutPathProcessor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(OutPathProcessor.class);

    private final String transfer_watch_file;
    private final String transfer_status_file;
    private final String incoming_directory;
    private final String outgoing_directory;
    private final String bucket_name;
    private Plugin plugin;

    public OutPathProcessor(Plugin plugin) {
        this.plugin = plugin;
        logger.debug("OutPathPreProcessor Instantiated");
        transfer_watch_file = plugin.getConfig().getStringParam("transfer_watch_file");
        logger.debug("\"pathstage4\" --> \"transfer_watch_file\" from config [{}]", transfer_watch_file);
        transfer_status_file = plugin.getConfig().getStringParam("transfer_status_file");
        logger.debug("\"pathstage4\" --> \"transfer_status_file\" from config [{}]", transfer_status_file);
        incoming_directory = plugin.getConfig().getStringParam("incoming_directory");
        logger.debug("\"pathstage4\" --> \"incoming_directory\" from config [{}]", incoming_directory);
        outgoing_directory = plugin.getConfig().getStringParam("outgoing_directory");
        logger.debug("\"pathstage4\" --> \"outgoing_directory\" from config [{}]", outgoing_directory);
        bucket_name = plugin.getConfig().getStringParam("bucket");
        logger.debug("\"pathstage4\" --> \"bucket\" from config [{}]", bucket_name);
    }

    @Override
    public void run() {
        logger.trace("Thread starting");
        try {
            logger.trace("Setting [PathProcessorActive] to true");
            plugin.PathProcessorActive = true;
            ObjectEngine oe = null;
            logger.trace("Entering while-loop");
            while (plugin.PathProcessorActive) {
                try {
                    oe = new ObjectEngine(plugin);
                    //oe.deleteBucketContents(bucket_name);
                    logger.trace("Populating [remoteDirs]");
                    List<String> remoteDirs = oe.listBucketDirs(bucket_name);
                    for(String remoteDir : remoteDirs) {
                        logger.debug("Remote Directory: *" + remoteDir +"*");
                    }
                    logger.trace("Populating [localDirs]");
                    List<String> localDirs = getWalkPath(incoming_directory);
                    for(String localDir : localDirs) {
                        logger.debug("Local Directory: *" + localDir + "*");
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
                }
            }
        } catch (Exception ex) {
            logger.error("run {}", ex.getMessage());
        }
    }

    private void processBucket(List<String> newDirs) {
        logger.debug("Call to processBucket [newDir = {}]", newDirs.toString());
        ObjectEngine oe = new ObjectEngine(plugin);

        for (String remoteDir : newDirs) {
            logger.debug("Downloading directory {} to [incoming_directory]", remoteDir);
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
                //process sample directories
                processDirectories(inDir,remoteDir);
            }

        }
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

    private void processDirectories(String dir, String remoteDir) {
        logger.trace("Processing Directory : " + dir);
        File file = new File(dir);
        String[] directories = file.list(new FilenameFilter() {
            @Override
            public boolean accept(File current, String name) {
                return new File(current, name).isDirectory();
            }
        });

        for (String subDir : directories) {
            logger.trace("Processing SubDirectory : " + subDir);
            String commands_main_filename = dir + subDir + "/commands_main.sh";
            String config_files_directoryname = dir + subDir + "/config_files";
            File commands_main = new File(commands_main_filename);
            File config_files = new File(config_files_directoryname);

            if (commands_main.exists() && !commands_main.isDirectory() && config_files.exists() && config_files.isDirectory()) {
                // do something
                logger.trace("Found : " + commands_main_filename + " and " + config_files_directoryname);

                UUID id = UUID.randomUUID(); //create random tmp location
                String tmpInput = dir + subDir;
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

            }
        }
    }

    private static void executeCommand(String command) {
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
                        logger.info("Log diff = " + logdiff + " : " +  outputStr[2] + " : " + outputStr[3] + " : " + outputStr[4]);
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
    }

    private void uploadProcessDir(String inDir, String outDir) {
        logger.debug("Call to uploadProcessDir [dir = {}]", inDir);

        ObjectEngine oe = new ObjectEngine(plugin);
        List<String> filterList = new ArrayList<>();
        //logger.trace("Adding [transfer_status_file] to [filterList]");
        //filterList.add(transfer_status_file);

            Map<String, String> md5map = oe.getDirMD5(inDir, filterList);
            logger.trace("Setting MD5 hash");
            setTransferFileMD5(inDir, md5map);
            logger.trace("Transferring directory");
            if (oe.uploadDirectory(bucket_name, inDir, outDir)) {
                    logger.debug("Directory Transfered [inDir = {}, outDir = {}]", inDir, outDir);
            }
    }

    private void setTransferFileMD5(String dir, Map<String, String> md5map) {
        logger.debug("Call to setTransferFileMD5 [dir = {}, md5map = {}]", dir, md5map);
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



