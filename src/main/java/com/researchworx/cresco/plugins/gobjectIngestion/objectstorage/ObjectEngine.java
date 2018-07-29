package com.researchworx.cresco.plugins.gobjectIngestion.objectstorage;

//import java.io.ByteArrayInputStream;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.*;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import com.amazonaws.util.StringUtils;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import com.researchworx.cresco.plugins.gobjectIngestion.Plugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ObjectEngine {
    private static final int MAX_FILES_FOR_S3_DOWNLOAD = 5000;
    private static final int NUMBER_OF_THREADS_FOR_DOWNLOAD = 10;

    private static final String bagit = "standard";
    private static final String hashing = "md5";
    private static final boolean hiddenFiles = true;
    private static final String compression = "gzip";

    private CLogger logger;
    private static AmazonS3 conn;
    //private final static String FOLDER_SUFFIX = "/";
    private MD5Tools md5t;
    private int partSize;
    private Plugin plugin;

    public ObjectEngine(Plugin plugin) {
        this.logger = new CLogger(ObjectEngine.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Trace);

        this.plugin = plugin;
        String accessKey = plugin.getConfig().getStringParam("accesskey");
        logger.debug("\"accesskey\" from config [{}]", accessKey);
        String secretKey = plugin.getConfig().getStringParam("secretkey");
        logger.debug("\"secretkey\" from config [{}]", secretKey);
        String endpoint = plugin.getConfig().getStringParam("endpoint");
        logger.debug("\"endpoint\" from config [{}]", endpoint);
        String s3Region = plugin.getConfig().getStringParam("s3region");
        logger.debug("\"s3Region\" from config [{}]", s3Region);

        this.partSize = plugin.getConfig().getIntegerParam("uploadpartsizemb");
        logger.debug("\"uploadpartsizemb\" from config [{}]", this.partSize);

        logger.trace("Building AWS Credentials");
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

        /*
        logger.trace("Building ClientConfiguration");
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setProtocol(Protocol.HTTPS);
        clientConfig.setSignerOverride("S3SignerType");
        clientConfig.setMaxConnections(plugin.getConfig().getIntegerParam("maxconnections", 50));
        */

        logger.trace("Connecting to Amazon S3");
        //BasicAWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey);
        //conn = AmazonS3ClientBuilder.standard().withRegion(s3Region).withCredentials(new AWSStaticCredentialsProvider(credentials)).withForceGlobalBucketAccessEnabled(true).build();

        /* Does not work with Ceph
        conn = AmazonS3ClientBuilder.standard().withRegion(s3Region).withCredentials(new AWSStaticCredentialsProvider(credentials)).withForceGlobalBucketAccessEnabled(true).build();
        */

        System.out.println("Building ClientConfiguration");
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setProtocol(Protocol.HTTPS);
        clientConfig.setSignerOverride("S3SignerType");
        //clientConfig.setMaxConnections(100);

        conn = new AmazonS3Client(credentials, clientConfig);
        //conn.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
        //S3ClientOptions(boolean pathStyleAccess, boolean chunkedEncodingDisabled, boolean accelerateModeEnabled, boolean payloadSigningEnabled)
        //conn.setS3ClientOptions(S3ClientOptions.);
        S3ClientOptions s3ops = S3ClientOptions.builder()
                .setAccelerateModeEnabled(false)
                .setPathStyleAccess(true)
                .setPayloadSigningEnabled(false)
                .build();
        conn.setS3ClientOptions(s3ops);

        conn.setEndpoint("https://iobjects.uky.edu");


        //conn.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
        //S3ClientOptions(boolean pathStyleAccess, boolean chunkedEncodingDisabled, boolean accelerateModeEnabled, boolean payloadSigningEnabled)
        //conn.setS3ClientOptions(S3ClientOptions.);

        //.withRegion(Regions.US_EAST_1)

        /*
        S3ClientOptions s3ops = S3ClientOptions.builder()
                .setAccelerateModeEnabled(false)
                .setPathStyleAccess(true)
                .setPayloadSigningEnabled(false)
                .build();
        conn.setS3ClientOptions(s3ops);


        conn.setEndpoint(endpoint);
        */

        logger.trace("Building new MD5Tools");
        md5t = new MD5Tools(plugin);

    }

    public boolean uploadBaggedSequence(String bucket, String inFile, String s3Prefix, String seqId, String sstep) {
        logger.debug("uploadBaggedSequence('{}','{}','{}','{}','{}')",
                bucket, inFile, s3Prefix, seqId, sstep);
        if (bucket == null || bucket.equals("") || !doesBucketExist(bucket)) {
            logger.error("You must supply a valid bucket name.");
            return false;
        }
        if (s3Prefix == null)
            s3Prefix = "";
        if (!s3Prefix.equals("") && !s3Prefix.endsWith("/"))
            s3Prefix += "/";
        logger.debug("Bagging up [{}]", inFile);
        String toUpload = Encapsulation.encapsulate(inFile, bagit, hashing, hiddenFiles, compression);
        if (toUpload == null) {
            logger.error("Failed to bag up [{}]", inFile);
            return false;
        }
        if (toUpload == null || toUpload.equals("") || !(new File(toUpload).exists())) {
            logger.error("You must supply something to upload.");
            return false;
        }
        boolean success = false;
        TransferManager manager = null;
        logger.trace("Building TransferManager");
        try {
            manager = TransferManagerBuilder.standard()
                    .withS3Client(conn)
                    .withMultipartUploadThreshold(1024L * 1024L * 5L)
                    .withMinimumUploadPartSize(1024L * 1024L * 5L)
                    .build();
            logger.trace("Checking that inDirectory exists");
            File uploadFile = new File(toUpload);
            if (!uploadFile.exists()) {
                logger.error("Input directory [{}] does not exist!");
                return false;
            }
            inFile = Paths.get(inFile).toString();
            logger.trace("New inFile: {}", inFile);
            logger.trace("file.seperator: {}", File.separatorChar);
            s3Prefix += inFile.substring((inFile.lastIndexOf(File.separatorChar) > -1 ? inFile.lastIndexOf(File.separatorChar) + 1 : 0));
            logger.trace("s3Prefix: {}", s3Prefix);
            PutObjectRequest request = new PutObjectRequest(bucket, s3Prefix, uploadFile);
            request.setGeneralProgressListener(new LoggingProgressListener(uploadFile.length()));
            logger.trace("Building upload montior and starting upload");
            //Upload transfer = manager.upload(bucket, s3Prefix, uploadFile);
            Upload transfer = manager.upload(request);
            UploadResult result = transfer.waitForUploadResult();
            String s3Checksum = result.getETag();
            logger.trace("s3Checksum: {}", result.getETag());
            String localChecksum;
            if (s3Checksum.contains("-"))
                localChecksum = md5t.getMultiCheckSum(inFile);
            else
                localChecksum = md5t.getCheckSum(inFile);
            logger.trace("localChecksum: {}", localChecksum);
            if (!localChecksum.equals(result.getETag()))
                logger.error("Checksums don't match [local: {}, S3: {}]", localChecksum, result.getETag());
            if (!compression.equals("none") && (toUpload.endsWith(".tar") || toUpload.endsWith(".tar.bz2") ||
                    toUpload.endsWith(".tar.gz") || toUpload.endsWith(".tar.xz") ||
                    toUpload.endsWith(".zip")))
                Files.delete(new File(toUpload).toPath());
            success = localChecksum.equals(result.getETag());
        } catch (AmazonServiceException ase) {
            logger.error("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            logger.error("Error Message:    " + ase.getMessage());
            logger.error("HTTP Status Code: " + ase.getStatusCode());
            logger.error("AWS Error Code:   " + ase.getErrorCode());
            logger.error("Error Type:       " + ase.getErrorType());
            logger.error("Request ID:       " + ase.getRequestId());
        } catch (SdkClientException ace) {
            logger.error("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            logger.error("Error Message: " + ace.getMessage());
        } catch (InterruptedException ie) {
            logger.error("Interrupted error");
        } catch (IOException ioe) {
            logger.error("IOException error: {}", ioe.getMessage());
        } finally {
            try {
                assert manager != null;
                manager.shutdownNow();
            } catch (AssertionError ae) {
                logger.error("uploadFile : TransferManager was pre-emptively shut down.");
                return false;
            }
        }
        return success;
    }

    public boolean uploadDirectory(String bucket, String inDir, String outDir) {
        logger.debug("Call to uploadDirectory [bucket = {}, inDir = {}, outDir = {}]", bucket, inDir, outDir);
        boolean wasTransfered = false;
        TransferManager tx = null;

        try {
            logger.trace("Building new TransferManager");
            tx = new TransferManager(conn);

            logger.trace("Building new TransferManagerConfiguration");
            TransferManagerConfiguration tmConfig = new TransferManagerConfiguration();
            logger.trace("Setting up minimum part size");

            // Sets the minimum part size for upload parts.
            tmConfig.setMinimumUploadPartSize(partSize * 1024 * 1024);
            logger.trace("Setting up size threshold for multipart uploads");
            // Sets the size threshold in bytes for when to use multipart uploads.
            tmConfig.setMultipartUploadThreshold((long) partSize * 1024 * 1024);
            logger.trace("Setting configuration on TransferManager");
            tx.setConfiguration(tmConfig);

            logger.trace("[uploadDir] set to [inDir]");
            File uploadDir = new File(inDir);

            logger.trace("Starting timer");
            long startUpload = System.currentTimeMillis();

            logger.info("Beginning upload [bucket = {}, outDir = {}, uploadDir = {}]", bucket, outDir, uploadDir);
            MultipleFileUpload myUpload = tx.uploadDirectory(bucket, outDir, uploadDir, true);

            DecimalFormat percentFormatter = new DecimalFormat("#.##");

            while (!myUpload.isDone()) {
                Thread.sleep(5000);

                logger.debug("Transfer: " + myUpload.getDescription());
                logger.debug("  - State: " + myUpload.getState());
                logger.debug("  - Progress Bytes: "
                        + myUpload.getProgress().getBytesTransferred());

                float transferTime = (System.currentTimeMillis() - startUpload) / 1000;
                long bytesTransfered = myUpload.getProgress().getBytesTransferred();
                float transferRate = (bytesTransfered / 1000000) / transferTime;

                logger.debug("Upload Transfer Desc: " + myUpload.getDescription());
                logger.debug(" - Transfered : " + myUpload.getProgress().getBytesTransferred() + " bytes");
                logger.debug(" - Elapsed time : " + (transferTime < 60 ? ((int)transferTime + " seconds") : ((int)(transferTime / 60) + " minutes " + ((transferTime % 60 > 0) ? transferTime % 60 + " seconds" : ""))));
                logger.debug(" - Transfer rate : " + transferRate + " MB/sec");

                MsgEvent me = plugin.genGMessage(MsgEvent.Type.INFO, "Transfer in progress (" + percentFormatter.format(myUpload.getProgress().getPercentTransferred()) + "%)");
                me.setParam("indir", inDir);
                me.setParam("outdir", outDir);
                me.setParam("seq_id", outDir);
                me.setParam("pathstage", String.valueOf(plugin.pathStage));
                me.setParam("sstep", "1");
                me.setParam("xfer_rate", String.valueOf(transferRate));
                me.setParam("xfer_bytes", String.valueOf(myUpload.getProgress().getBytesTransferred()));
                me.setParam("xfer_percent", String.valueOf(myUpload.getProgress().getPercentTransferred()));
                plugin.sendMsgEvent(me);
            }
            wasTransfered = true;
        } catch (Exception ex) {
            logger.error("uploadDirectory {}", ex.getMessage());
        } finally {
            try {
                assert tx != null;
                tx.shutdownNow();
            } catch (AssertionError e) {
                logger.error("uploadDirectory - TransferManager was pre-emptively shutdown");
            }
        }
        return wasTransfered;
    }

    public boolean uploadSequenceDirectory(String bucket, String inDir, String outDir, String seqId, String sstep) {
        logger.debug("Call to uploadSequenceDirectory [bucket = {}, inDir = {}, outDir = {}, seqId = {}, sstep = {}]", bucket, inDir, outDir, seqId, sstep);
        boolean wasTransfered = false;
        TransferManager tx = null;

        try {
            logger.trace("Building new TransferManager");
            tx = new TransferManager(conn);

            logger.trace("Building new TransferManagerConfiguration");
            TransferManagerConfiguration tmConfig = new TransferManagerConfiguration();
            logger.trace("Setting up minimum part size");

            // Sets the minimum part size for upload parts.
            tmConfig.setMinimumUploadPartSize(partSize * 1024 * 1024);
            logger.trace("Setting up size threshold for multipart uploads");
            // Sets the size threshold in bytes for when to use multipart uploads.
            tmConfig.setMultipartUploadThreshold((long) partSize * 1024 * 1024);
            logger.trace("Setting configuration on TransferManager");
            tx.setConfiguration(tmConfig);

            logger.trace("[uploadDir] set to [inDir]");
            File uploadDir = new File(inDir);

            logger.trace("Starting timer");
            long startUpload = System.currentTimeMillis();

            logger.info("Beginning upload [bucket = {}, outDir = {}, uploadDir = {}]", bucket, outDir, uploadDir);
            MultipleFileUpload myUpload = tx.uploadDirectory(bucket, outDir, uploadDir, true);

            DecimalFormat percentFormatter = new DecimalFormat("#.##");

            while (!myUpload.isDone()) {
                Thread.sleep(5000);

                logger.debug("Transfer: " + myUpload.getDescription());
                logger.debug(" - State: " + myUpload.getState());
                logger.debug(" - Progress Bytes: "
                        + myUpload.getProgress().getBytesTransferred());

                float transferTime = (System.currentTimeMillis() - startUpload) / 1000;
                long bytesTransfered = myUpload.getProgress().getBytesTransferred();
                float transferRate = (bytesTransfered / 1000000) / transferTime;

                logger.debug("Upload Transfer Desc: " + myUpload.getDescription());
                logger.debug(" - Transfered : " + humanReadableByteCount(myUpload.getProgress().getBytesTransferred(), true) + " (" + percentFormatter.format(myUpload.getProgress().getPercentTransferred()) + "%)");
                logger.debug(" - Elapsed time : " + (transferTime < 60 ? ((int)transferTime + " seconds") : ((int)(transferTime / 60) + " minutes " + ((transferTime % 60 > 0) ? transferTime % 60 + " seconds" : ""))));
                logger.debug(" - Transfer rate : " + percentFormatter.format(transferRate) + " MB/sec");

                MsgEvent me = plugin.genGMessage(MsgEvent.Type.INFO, "Transfer in progress (" + percentFormatter.format(myUpload.getProgress().getPercentTransferred()) + "%)");
                me.setParam("indir", inDir);
                me.setParam("outdir", outDir);
                me.setParam("seq_id", seqId);
                me.setParam("pathstage", String.valueOf(plugin.pathStage));
                me.setParam("sstep", sstep);
                me.setParam("xfer_rate", String.valueOf(transferRate));
                me.setParam("xfer_bytes", String.valueOf(myUpload.getProgress().getBytesTransferred()));
                me.setParam("xfer_percent", String.valueOf(myUpload.getProgress().getPercentTransferred()));
                plugin.sendMsgEvent(me);
            }
            wasTransfered = true;
        } catch (Exception ex) {
            logger.error("uploadDirectory {}", ex.getMessage());
        } finally {
            try {
                assert tx != null;
                tx.shutdownNow();
            } catch (AssertionError e) {
                logger.error("uploadDirectory - TransferManager was pre-emptively shutdown");
            }
        }
        return wasTransfered;
    }

    public boolean uploadSampleDirectory(String bucket, String inDir, String outDir, String seqId, String sampleId, String ssstep) {
        logger.debug("Call to uploadSampleDirectory [bucket = {}, inDir = {}, outDir = {}, seqId = {}, sampleId = {}, ssstep = {}]", bucket, inDir, outDir, seqId, sampleId, ssstep);
        boolean wasTransfered = false;
        TransferManager tx = null;

        try {
            logger.trace("Building new TransferManager");
            tx = new TransferManager(conn);

            logger.trace("Building new TransferManagerConfiguration");
            TransferManagerConfiguration tmConfig = new TransferManagerConfiguration();
            logger.trace("Setting up minimum part size");

            // Sets the minimum part size for upload parts.
            tmConfig.setMinimumUploadPartSize(partSize * 1024 * 1024);
            logger.trace("Setting up size threshold for multipart uploads");
            // Sets the size threshold in bytes for when to use multipart uploads.
            tmConfig.setMultipartUploadThreshold((long) partSize * 1024 * 1024);
            logger.trace("Setting configuration on TransferManager");
            tx.setConfiguration(tmConfig);

            logger.trace("[uploadDir] set to [inDir]");
            File uploadDir = new File(inDir);

            logger.trace("Starting timer");
            long startUpload = System.currentTimeMillis();

            logger.info("Beginning upload [bucket = {}, outDir = {}, uploadDir = {}]", bucket, outDir, uploadDir);
            MultipleFileUpload myUpload = tx.uploadDirectory(bucket, outDir, uploadDir, true);

            DecimalFormat percentFormatter = new DecimalFormat("#.##");

            while (!myUpload.isDone()) {
                Thread.sleep(5000);

                logger.debug("Transfer: " + myUpload.getDescription());
                logger.debug("  - State: " + myUpload.getState());
                logger.debug("  - Progress Bytes: "
                        + myUpload.getProgress().getBytesTransferred());

                float transferTime = (System.currentTimeMillis() - startUpload) / 1000;
                long bytesTransfered = myUpload.getProgress().getBytesTransferred();
                float transferRate = (bytesTransfered / 1000000) / transferTime;

                logger.debug("Upload Transfer Desc: " + myUpload.getDescription());
                logger.debug(" - Transfered : " + humanReadableByteCount(myUpload.getProgress().getBytesTransferred(), true) + " (" + percentFormatter.format(myUpload.getProgress().getPercentTransferred()) + "%)");
                logger.debug(" - Elapsed time : " + (transferTime < 60 ? ((int)transferTime + " seconds") : ((int)(transferTime / 60) + " minutes " + ((transferTime % 60 > 0) ? transferTime % 60 + " seconds" : ""))));
                logger.debug(" - Transfer rate : " + percentFormatter.format(transferRate) + " MB/sec");

                MsgEvent me = plugin.genGMessage(MsgEvent.Type.INFO, "Transfer in progress (" + percentFormatter.format(myUpload.getProgress().getPercentTransferred()) + "%)");
                me.setParam("indir", inDir);
                me.setParam("outdir", outDir);
                me.setParam("seq_id", seqId);
                me.setParam("sample_id", sampleId);
                me.setParam("pathstage", String.valueOf(plugin.pathStage));
                me.setParam("ssstep", ssstep);
                me.setParam("xfer_rate", String.valueOf(transferRate));
                me.setParam("xfer_bytes", String.valueOf(myUpload.getProgress().getBytesTransferred()));
                me.setParam("xfer_percent", String.valueOf(myUpload.getProgress().getPercentTransferred()));
                plugin.sendMsgEvent(me);
            }
            wasTransfered = true;
        } catch (Exception ex) {
            logger.error("uploadDirectory {}", ex.getMessage());
        } finally {
            try {
                assert tx != null;
                tx.shutdownNow();
            } catch (AssertionError e) {
                logger.error("uploadDirectory - TransferManager was pre-emptively shutdown");
            }
        }
        return wasTransfered;
    }

    public boolean downloadDirectory(String bucketName, String keyPrefix, String destinationDirectory, String seqId, String sampleId) {
        logger.debug("Call to downloadDirectory [bucketName = {}, keyPrefix = {}, destinationDirectory = {}", bucketName, keyPrefix, destinationDirectory);
        boolean wasTransfered = false;
        if (!destinationDirectory.endsWith("/")) {
            destinationDirectory += "/";
        }

        logger.trace("Setting [downloadDir] to [desinationDirectory]");
        File downloadDir = new File(destinationDirectory);
        if (!downloadDir.exists()) {
            if (!downloadDir.mkdirs()) {
                logger.error("Failed to create download directory!");
                return false;
            }
        }

        Map<String, Long> dirList = getlistBucketContents(bucketName, keyPrefix);

        if (dirList.keySet().size() <= MAX_FILES_FOR_S3_DOWNLOAD) {
            TransferManager tx = null;
            try {
                logger.trace("Building new TransferManager");
                tx = new TransferManager(conn);

                logger.trace("Starting download timer");
                long startDownload = System.currentTimeMillis();
                logger.info("Beginning download [bucketName = {}, keyPrefix = {}, downloadDir = {}", bucketName, keyPrefix, downloadDir);
                MultipleFileDownload myDownload = tx.downloadDirectory(bucketName, keyPrefix, downloadDir);

                DecimalFormat percentFormatter = new DecimalFormat("#.##");
                while (!myDownload.isDone()) {
                    Thread.sleep(5000);

                    logger.debug("Transfer: " + myDownload.getDescription());
                    logger.debug("  State: " + myDownload.getState());
                    logger.debug("  Progress Bytes: "
                            + myDownload.getProgress().getBytesTransferred());

                    float transferTime = (System.currentTimeMillis() - startDownload) / 1000;
                    long bytesTransfered = myDownload.getProgress().getBytesTransferred();
                    float transferRate = (bytesTransfered / 1000000) / transferTime;

                    logger.debug("Download Transfer Desc: " + myDownload.getDescription());
                    logger.debug("  Transfered : " + myDownload.getProgress().getBytesTransferred() + " bytes");
                    logger.debug("  Elapsed time : " + transferTime + " seconds");
                    logger.debug("  Transfer rate : " + transferRate + " MB/sec");

                    MsgEvent me = plugin.genGMessage(MsgEvent.Type.INFO, "Transfer in progress (" + percentFormatter.format(myDownload.getProgress().getPercentTransferred()) + "%)");
                    if (seqId != null)
                        me.setParam("seq_id", seqId);
                    if (sampleId != null)
                        me.setParam("sample_id", sampleId);
                    me.setParam("pathstage", String.valueOf(plugin.pathStage));
                    if (sampleId == null)
                        me.setParam("sstep", "1");
                    else
                        me.setParam("ssstep", "1");
                    me.setParam("xfer_rate", String.valueOf(transferRate));
                    me.setParam("xfer_bytes", String.valueOf(myDownload.getProgress().getBytesTransferred()));
                    me.setParam("xfer_percent", String.valueOf(myDownload.getProgress().getPercentTransferred()));
                    plugin.sendMsgEvent(me);
                }

                logger.trace("Calculating download statistics");
                float transferTime = (System.currentTimeMillis() - startDownload) / 1000;
                long bytesTransfered = myDownload.getProgress().getBytesTransferred();
                float transferRate = (bytesTransfered / 1000000) / transferTime;

                logger.debug("Download Transfer Desc: " + myDownload.getDescription());
                logger.debug(" - Transfered : " + humanReadableByteCount(myDownload.getProgress().getBytesTransferred(), true) + " (" + percentFormatter.format(myDownload.getProgress().getPercentTransferred()) + "%)");
                logger.debug(" - Elapsed time : " + (transferTime < 60 ? ((int)transferTime + " seconds") : ((int)(transferTime / 60) + " minutes " + ((transferTime % 60 > 0) ? transferTime % 60 + " seconds" : ""))));
                logger.debug(" - Transfer rate : " + transferRate + " MB/sec");

                wasTransfered = true;
            } catch (Exception ex) {
                logger.error("downloadDirectory {}", ex.getMessage());
            } finally {
                try {
                    assert tx != null;
                    tx.shutdownNow();
                } catch (AssertionError e) {
                    logger.error("downloadDirectory - TransferManager was pre-emptively shutdown");
                }
            }
        } else {
            try {
                logger.trace("Beginning Large FileSet Download routine");
                ExecutorService downloadExecutorService = Executors.newFixedThreadPool(NUMBER_OF_THREADS_FOR_DOWNLOAD);
                long totalBytesToDownload = 0L;
                ConcurrentHashMap<String, Long> downloadProgresses = new ConcurrentHashMap<>();
                CountDownLatch latch = new CountDownLatch(1);
                logger.debug("dirList.size() = {}", dirList.size());
                logger.trace("Creating directories and building downloads");
                for (Map.Entry<String, Long> entry : dirList.entrySet()) {
                    if (entry.getKey() == null) continue;
                    String dir = entry.getKey();
                    String tdir = dir.substring(0, dir.lastIndexOf("/"));
                    File directory = new File(destinationDirectory + tdir);
                    File file = new File(destinationDirectory + dir);

                    if (!directory.exists()) {
                        if (createDir(directory)) {
                            logger.trace("created directory : " + directory.getAbsolutePath());
                        } else {
                            logger.error("failed creating directory : " + directory.getAbsolutePath());
                        }
                    }
                    if (entry.getValue() != null)
                        totalBytesToDownload += entry.getValue();
                    downloadExecutorService.submit(new DownloadWorker(bucketName, dir, file, downloadProgresses, latch, logger));
                }
                logger.trace("All downloads have been generated");
                downloadExecutorService.shutdown();
                logger.trace("Triggering downloads to start");
                long startDownload = System.currentTimeMillis();
                latch.countDown();
                logger.trace("Entering Status while loop.");
                DecimalFormat percentFormatter = new DecimalFormat("#.##");
                while (!downloadExecutorService.isTerminated()) {
                    Thread.sleep(60000);
                    long totalBytesDownloaded = 0L;
                    for (Map.Entry<String, Long> entry : downloadProgresses.entrySet()) {
                        totalBytesDownloaded += entry.getValue();
                    }
                    logger.trace("Calculating download progress metrics.");
                    float transferTime = (System.currentTimeMillis() - startDownload) / 1000;
                    float transferRate = 0;
                    if (transferTime > 0)
                        transferRate = (totalBytesDownloaded / 1000000) / transferTime;
                    double progress = 0;
                    if (totalBytesToDownload > 0L)
                        progress = ((double) totalBytesDownloaded / (double) totalBytesToDownload) * 100.0;
                    logger.trace("Sending download progress metrics to controller");
                    logger.debug(" - Transferred: {} / {} ({}%)", humanReadableByteCount(totalBytesDownloaded, true), humanReadableByteCount(totalBytesToDownload, true), percentFormatter.format(progress));
                    logger.debug(" - Elapsed time : " + (transferTime < 60 ? ((int)transferTime + " seconds") : ((int)(transferTime / 60) + " minutes " + ((transferTime % 60 > 0) ? transferTime % 60 + " seconds" : ""))));
                    logger.debug(" - Transfer rate: {} MB/sec", transferRate);
                    MsgEvent me = plugin.genGMessage(MsgEvent.Type.INFO, "Transfer in progress (" + percentFormatter.format(progress) + "%)");
                    if (seqId != null)
                        me.setParam("seq_id", seqId);
                    if (sampleId != null)
                        me.setParam("sample_id", sampleId);
                    me.setParam("pathstage", String.valueOf(plugin.pathStage));
                    if (sampleId == null)
                        me.setParam("sstep", "1");
                    else
                        me.setParam("ssstep", "1");
                    me.setParam("xfer_rate", String.valueOf(transferRate));
                    me.setParam("xfer_bytes", String.valueOf(totalBytesDownloaded));
                    me.setParam("xfer_percent", String.valueOf(progress));
                    plugin.sendMsgEvent(me);
                }

                wasTransfered = true;
            } catch (InterruptedException e) {
                return false;
            } catch (Exception e) {
                logger.error("Exception in Large FileSet Download routine: {}", e.getMessage());
            }
        }
        return wasTransfered;
    }

    private Map<String, Long> getlistBucketContents(String bucket, String prefixKey) {
        if (!prefixKey.endsWith("/"))
            prefixKey = prefixKey + "/";
        logger.debug("Call to listBucketContents [bucket = {}, prefixKey = {}]", bucket, prefixKey);
        Map<String, Long> dirList = new HashMap<>();
        try {
            if (doesBucketExist(bucket)) {
                logger.trace("Grabbing [objects] list from [bucket]");
                for (S3ObjectSummary objectSummary : S3Objects.withPrefix(conn, bucket, prefixKey)) {
                    dirList.put(objectSummary.getKey(), objectSummary.getSize());
                }
            } else {
                logger.warn("Bucket :" + bucket + " does not exist!");
            }
        } catch (Exception ex) {
            logger.error("getlistBucketContents {}", ex.getMessage());
        }
        return dirList;
    }

    private class DownloadWorker implements Runnable {
        private String keyPrefix;
        private String bucketName;
        private File file;
        private Map<String, Long> downloadProgresses;
        private CountDownLatch latch;
        private CLogger logger;

        DownloadWorker(String bucketName, String keyPrefix, File file, Map<String, Long> downloadProgresses, CountDownLatch latch, CLogger logger) {
            this.keyPrefix = keyPrefix;
            this.bucketName = bucketName;
            this.file = file;
            this.downloadProgresses = downloadProgresses;
            this.latch = latch;
            this.logger = logger;
        }

        @Override
        public void run() {
            try {
                latch.await();
                S3Object object = conn.getObject(new GetObjectRequest(bucketName, keyPrefix));
                InputStream objectData = object.getObjectContent();
                OutputStream outStream = new FileOutputStream(file);
                int n;
                long bytesDownloaded = 0L;
                byte[] buffer = new byte[16384];

                while ((n = objectData.read(buffer)) > -1) {
                    bytesDownloaded += n;
                    downloadProgresses.put(keyPrefix, bytesDownloaded);
                    outStream.write(buffer, 0, n);
                }

                outStream.close();
                objectData.close();
                object.close();
            } catch (Exception ex) {
                logger.error("Failed to download file [{}] due to: {}", file.getAbsolutePath());
            }
        }
    }

    private static boolean createDir(File file) {
        boolean isCreated = false;
        try {
            //File file = new File(directories);

            // The mkdirs will create folder including any necessary but non existence
            // parent directories. This method returns true if and only if the directory
            // was created along with all necessary parent directories.
            isCreated = file.mkdirs();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return isCreated;
    }

    /*public void createFolder(String bucket, String foldername) {

        // Create metadata for your folder & set content-length to 0
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);

        // Create empty content
        InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

        // Create a PutObjectRequest passing the foldername suffixed by /
        PutObjectRequest putObjectRequest =
                new PutObjectRequest(bucket, foldername + FOLDER_SUFFIX,
                        emptyContent, metadata);

        // Send request to S3 to create folder
        conn.putObject(putObjectRequest);
    }*/

    public boolean isSyncDir(String bucket, String s3Dir, String localDir, List<String> ignoreList) {
        logger.debug("Call to isSyncDir [bucket = {}, s3Dir = {}, localDir = {}, ignoreList = {}", bucket, s3Dir, localDir, ignoreList.toString());
        boolean isSync = true;
        Map<String, String> mdhp = new HashMap<>();

        try {
            if (!s3Dir.endsWith("/")) {
                s3Dir = s3Dir + "/";
            }
            //check if bucket exist
            if (doesBucketExist(bucket)) {
                logger.trace("isSync Grabbing [objects] from [bucket] [s3Dir]");
                /*ObjectListing objects = conn.listObjects(bucket, s3Dir);
                do {
                    for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                        if (!mdhp.containsKey(objectSummary.getKey())) {
                            logger.trace("Adding from s3 [{} : {}]", objectSummary.getKey(), objectSummary.getETag());
                            mdhp.put(objectSummary.getKey(), objectSummary.getETag());
                        }
                    }
                    logger.trace("Grabbing next batch of [objects]");
                    objects = conn.listNextBatchOfObjects(objects);
                } while (objects.isTruncated());*/

                for (S3ObjectSummary objectSummary : S3Objects.withPrefix(conn, bucket, s3Dir)) {
                    mdhp.put(objectSummary.getKey(), objectSummary.getETag());
                }

                //S3Object object = conn.getObject(new GetObjectRequest(bucketName, key));

                logger.trace("Grabbing list of files from [localDir]");
                File folder = new File(localDir);
                File[] listOfFiles = folder.listFiles();
                for (File file : listOfFiles != null ? listOfFiles : new File[0]) {
                    if ((file.isFile()) && (!ignoreList.contains(file.getName()))) {
                        String bucket_key = s3Dir + file.getName();
                        logger.debug("[bucket_key = {}]", bucket_key);
                        String md5hash;
                        if (mdhp.containsKey(bucket_key)) {
                            logger.debug("[mdhp] contains [bucket_key]");
                            String checkhash = mdhp.get(bucket_key);
                            if (checkhash.contains("-")) {
                                logger.debug("Grabbing multipart-checksum for large/multipart file");
                                md5hash = md5t.getMultiCheckSum(file.getAbsolutePath());
                            } else {
                                logger.debug("Grabbing direct checksum for small/non-multipart file");
                                md5hash = md5t.getCheckSum(file.getAbsolutePath());
                            }
                            if (!md5hash.equals(checkhash)) {
                                isSync = false;
                                logger.debug("Invalid Sync [bucket_key = {}, checkhash = {}] should be [md5hash = {}]", bucket_key, checkhash, md5hash);
                            }
                        } else {
                            logger.debug("Missing Key [bucket_key = {}]", bucket_key);
                            isSync = false;
                        }
                    }
                }

            } else {
                logger.warn("Bucket :" + bucket + " does not exist!");

            }
        } catch (Exception ex) {
            logger.error("isSyncDir {}", ex.getMessage());
            isSync = false;
        }
        mdhp.clear();
        return isSync;

    }

    public boolean setDirMD5File(String localDir, List<String> ignoreList) {
        if(!localDir.endsWith("/")) {
            localDir += "/";
        }
        logger.debug("Call to getDirMD5 [localDir = {}]", localDir);

        PrintWriter writer = null;
        try {
            writer = new PrintWriter(localDir + "md5hashes.txt", "UTF-8");
            logger.trace("Grabbing [listOfFiles] from [localDir]");
            File folder = new File(localDir);
            for (File file : folder.listFiles()) {
                if ((file.isFile()) && (!ignoreList.contains(file.getName()))) {
                    writer.println(file.getAbsolutePath() + "," + md5t.getCheckSum(file.getAbsolutePath()));
                }
            }
        } catch (Exception ex) {
            logger.error("setDirMD5File {}", ex.getMessage());
            return false;
        }
        writer.close();
        return true;
    }

    public Map<String, String> getDirMD5(String localDir, List<String> ignoreList) {
        logger.debug("Call to getDirMD5 [localDir = {}, ignoreList = {}]", localDir, ignoreList.toString());
        Map<String, String> mdhp = new HashMap<>();

        try {
            logger.trace("Grabbing [listOfFiles] from [localDir]");
            File folder = new File(localDir);
            File[] listOfFiles = folder.listFiles();
            logger.trace("Iterating [listOfFiles]");
            for (File file : listOfFiles != null ? listOfFiles : new File[0]) {
                if ((file.isFile()) && (!ignoreList.contains(file.getName()))) {
                    logger.trace("Processing [file = {}]", file.toString());
                    if (!ignoreList.contains(file.getName())) {
                        logger.trace("Adding [file] to [mdhp]");
                        mdhp.put(file.getAbsolutePath(), md5t.getCheckSum(file.getAbsolutePath()));
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("isSyncDir {}", ex.getMessage());
        }
        return mdhp;

    }

    public Map<String, String> listBucketContents(String bucket) {
        logger.debug("Call to listBucketContents [bucket = {}]", bucket);
        Map<String, String> fileMap = new HashMap<>();
        try {
            if (doesBucketExist(bucket)) {
                logger.trace("Grabbing [objects] list from [bucket]");
                ObjectListing objects = conn.listObjects(bucket);
                do {
                    for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                        logger.debug("Found object {}\t{}\t{}\t{}", objectSummary.getKey(),
                                objectSummary.getSize(),
                                objectSummary.getETag(),
                                StringUtils.fromDate(objectSummary.getLastModified()));
                        fileMap.put(objectSummary.getKey(), objectSummary.getETag());
                    }
                    logger.trace("Grabbing next batch of [objects]");
                    objects = conn.listNextBatchOfObjects(objects);
                } while (objects.isTruncated());
            } else {
                logger.warn("Bucket :" + bucket + " does not exist!");
            }
        } catch (Exception ex) {
            logger.error("listBucketContents {}", ex.getMessage());
            fileMap = null;
        }
        return fileMap;
    }

    public List<String> listBucketDirs(String bucket) {
        logger.debug("Call to listBucketDirs [bucket = {}]", bucket);
        List<String> dirList = new ArrayList<>();
        try {
            if (doesBucketExist(bucket)) {
                logger.trace("Instantiating new ListObjectsRequest");
                ListObjectsRequest lor = new ListObjectsRequest();
                lor.setBucketName(bucket);
                lor.setDelimiter("/");

                logger.trace("Grabbing [objects] list from [lor]");
                //if(doesBucketExist(bucket))
                ObjectListing objects = conn.listObjects(lor);
                do {
                    List<String> sublist = objects.getCommonPrefixes();
                    logger.trace("Adding all Common Prefixes from [objects]");
                    dirList.addAll(sublist);
                    logger.trace("Grabbing next batch of [objects]");
                    objects = conn.listNextBatchOfObjects(objects);
                } while (objects.isTruncated());
            } else {
                logger.warn("Bucket :" + bucket + " does not exist!");
            }
        } catch (Exception ex) {
            logger.error("listBucketDirs {}", ex.getMessage());
            dirList = null;
        }
        return dirList;
    }

    public Map<String, String> listBucketContents(String bucket, String searchName) {
        logger.debug("Call to listBucketContents [bucket = {}, searchName = {}]", bucket, searchName);
        Map<String, String> fileMap = new HashMap<>();
        try {
            if (doesBucketExist(bucket)) {
                logger.trace("Grabbing [objects] list from [bucket]");
                ObjectListing objects = conn.listObjects(bucket);
                do {
                    for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                        if (objectSummary.getKey().contains(searchName)) {
                            logger.debug("Found object {}\t{}\t{}\t{}", objectSummary.getKey(),
                                    objectSummary.getSize(),
                                    objectSummary.getETag(),
                                    StringUtils.fromDate(objectSummary.getLastModified()));
                            fileMap.put(objectSummary.getKey(), objectSummary.getETag());
                        }
                    }
                    objects = conn.listNextBatchOfObjects(objects);
                } while (objects.isTruncated());
            } else {
                logger.warn("Bucket :" + bucket + " does not exist!");
            }
        } catch (Exception ex) {
            logger.error("listBucketContents {}", ex.getMessage());
            fileMap = null;
        }
        return fileMap;
    }

    public boolean doesObjectExist(String bucket, String objectName) {
        logger.debug("Call to doesObjectExist [bucket = {}, objectName = {}]", bucket, objectName);
        return conn.doesObjectExist(bucket, objectName);
    }

    public boolean doesBucketExist(String bucket) {
        logger.debug("Call to doesBucketExist [bucket = {}]", bucket);
        return conn.doesBucketExist(bucket);
    }

    public void createBucket(String bucket) {
        logger.debug("Call to createBucket [bucket = {}]", bucket);
        try {
            if (!conn.doesBucketExist(bucket)) {
                Bucket mybucket = conn.createBucket(bucket);
                logger.debug("Created bucket [{}] ", bucket);
            }
        } catch (Exception ex) {
            logger.error("createBucket {}", ex.getMessage());
        }

    }

    public void deleteBucketContents(String bucket) {
        logger.debug("Call to deleteBucketContents [bucket = {}]", bucket);
        logger.trace("Grabbing [objects] list from [bucket]");
        ObjectListing objects = conn.listObjects(bucket);
        do {
            for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                logger.trace("Deleting [{}] object from [{}] bucket", objectSummary.getKey(), bucket);
                conn.deleteObject(bucket, objectSummary.getKey());

                logger.debug("Deleted {}\t{}\t{}\t{}", objectSummary.getKey(),
                        objectSummary.getSize(),
                        objectSummary.getETag(),
                        StringUtils.fromDate(objectSummary.getLastModified()));
            }
            objects = conn.listNextBatchOfObjects(objects);
        } while (objects.isTruncated());
    }

    public void deleteBucketDirectoryContents(String bucket, String prefixKey) {
        if (!prefixKey.endsWith("/"))
            prefixKey = prefixKey + "/";
        logger.debug("Call to deleteBucketDirectoryContents [bucket = {}, prefixKey = {}]", bucket, prefixKey);
        try {
            if (doesBucketExist(bucket)) {
                for (S3ObjectSummary objectSummary : S3Objects.withPrefix(conn, bucket, prefixKey)) {
                    logger.trace("Deleting [{}] from bucket [{}]", objectSummary.getKey(), bucket);
                    conn.deleteObject(bucket, objectSummary.getKey());
                }
            }
        } catch (Exception e) {
            logger.error("deleteBucketDirectoryContents {}", e.getMessage());
        }
    }

    private class LoggingProgressListener implements ProgressListener {
        private final Logger logger = LoggerFactory.getLogger(LoggingProgressListener.class);
        private int updatePercentStep = 5;
        private long startTimestamp = 0L;
        private long totalTransferred = 0L;
        private long totalBytes = 0L;
        private int nextUpdate = updatePercentStep;
        private DecimalFormat toPercent = new DecimalFormat("#.##");

        public LoggingProgressListener(long totalBytes) {
            this.startTimestamp = System.currentTimeMillis();
            this.totalBytes = totalBytes;
        }

        @Override
        public void progressChanged(ProgressEvent progressEvent) {
            Thread.currentThread().setName("TransferListener");
            long currentTimestamp = System.currentTimeMillis();
            long transferTime = (currentTimestamp - startTimestamp) / 1000L;
            long currentBytesTransferred = progressEvent.getBytesTransferred();
            float currentTransferRate = (totalTransferred / (float)1000000) / transferTime;
            this.totalTransferred += currentBytesTransferred;
            float currentTransferPercentage = ((float)totalTransferred / (float)totalBytes) * (float)100;
            if (currentTransferPercentage > (float)nextUpdate) {
                logger.debug("Transferred in progress ({}/{} {}%) at {} MB/s",
                        humanReadableByteCount(totalTransferred, true), humanReadableByteCount(totalBytes, true),
                        (int)currentTransferPercentage, toPercent.format(currentTransferRate));
                nextUpdate += updatePercentStep;
            }
        }
    }

    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }
}