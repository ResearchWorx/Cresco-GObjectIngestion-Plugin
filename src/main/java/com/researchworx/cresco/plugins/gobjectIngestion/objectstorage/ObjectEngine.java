package com.researchworx.cresco.plugins.gobjectIngestion.objectstorage;

//import java.io.ByteArrayInputStream;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.*;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import com.amazonaws.util.StringUtils;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import com.researchworx.cresco.plugins.gobjectIngestion.Plugin;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
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
    private static final String compression = "tar";
    public static final String extension = ".tar";
    private static final boolean removeExisting = true;

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

        /*System.out.println("Building ClientConfiguration");
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

        conn.setEndpoint(endpoint);*/

        logger.trace("Building S3 client configuration");
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setProtocol(Protocol.HTTPS);
        clientConfiguration.setSignerOverride("S3SignerType");
        //clientConfiguration.setMaxConnections(maxConnections);
        logger.trace("Building S3 client");
        conn = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new EndpointConfiguration(endpoint, s3Region))
                .withClientConfiguration(clientConfiguration)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withAccelerateModeEnabled(false)
                .withPathStyleAccessEnabled(true)
                .withPayloadSigningEnabled(false)
                .build();


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

    public boolean uploadBaggedDirectory(String bucket, String inPath, String s3Prefix, String seqId, String sampleId,
                                         String reqId, String step) {
        logger.debug("uploadBaggedDirectory('{}','{}','{}','{}','{}','{}')",
                bucket, inPath, s3Prefix, seqId, sampleId, step);
        if (bucket == null || bucket.equals("") || !doesBucketExist(bucket)) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, step, "You must supply a valid bucket name");
            return false;
        }
        if (inPath == null || inPath.equals("")) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, step, "You must supply a valid directory folder to upload");
            return false;
        }
        File inFile = new File(inPath);
        if (!inFile.exists()) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, step, String.format("Directory to upload [%s] does not exist",
                    inFile.getAbsolutePath()));
            return false;
        }
        if (!inFile.isDirectory()) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, step, String.format("Directory to upload [%s] is not a directory",
                    inFile.getAbsolutePath()));
            return false;
        }
        long freeSpace = inFile.getUsableSpace();
        logger.trace("freeSpace: {}", freeSpace);
        long uncompressedSize = FileUtils.sizeOfDirectory(inFile);
        logger.trace("uncompressedSize: {}", uncompressedSize);
        long requiredSpace = uncompressedSize + (1024 * 1024 * 1024);
        logger.trace("requiredSpace: {}", requiredSpace);
        if (requiredSpace > freeSpace) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, step,
                    String.format("Not enough free space to bag up [%s], needs [%s] has [%s]",
                            inFile.getAbsolutePath(), humanReadableByteCount(requiredSpace, true),
                            humanReadableByteCount(freeSpace, true)));
            return false;
        }
        if (s3Prefix == null)
            s3Prefix = "";
        if (!s3Prefix.equals("") && !s3Prefix.endsWith("/"))
            s3Prefix += "/";
        sendUpdateInfoMessage(seqId, sampleId, reqId, step, String.format("Bagging up [%s]", inFile.getAbsolutePath()));
        File bagged = Encapsulation.bagItUp(inFile, bagit, hashing, hiddenFiles);
        if (bagged == null || !bagged.exists()) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, step, String.format("Failed to bag up directory [%s]",
                    inFile.getAbsolutePath()));
            return false;
        }
        sendUpdateInfoMessage(seqId, sampleId, reqId, step, String.format("Boxing up [%s]", inFile.getAbsolutePath()));
        //File boxed = Encapsulation.boxItUp(bagged, compression);
        File boxed = null;
        switch (compression) {
            case "tar":
                logger.trace("Using TAR archiving method");
                File packFile = new File(bagged.getAbsolutePath() + ".tar");
                try {
                    Encapsulation.pack(packFile, bagged);
                    boxed = packFile;
                } catch (IOException ioe) {
                    logger.error("[{}:{}]\n{}", ioe.getClass().getCanonicalName(), ioe.getMessage(),
                            ExceptionUtils.getStackTrace(ioe));
                    boxed = null;
                }
                break;
            case "gzip":
                logger.trace("Using GZIP archiving method");
                File compressFile = new File(bagged.getAbsolutePath() + ".tar.gz");
                try {
                    Encapsulation.compress(compressFile, bagged);
                    boxed = compressFile;
                } catch (IOException ioe) {
                    logger.error("[{}:{}]\n{}", ioe.getClass().getCanonicalName(), ioe.getMessage(),
                            ExceptionUtils.getStackTrace(ioe));
                    boxed = null;
                }
                break;
            case "none":
                logger.trace("No archiving requested");
                break;
            default:
                logger.error("Archive mode [{}] not currently supported", compression);
                boxed = null;
                break;
        }
        if (boxed == null || !boxed.exists()) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, step, String.format("Failed to box up directory [%s]",
                    inFile.getAbsolutePath()));
            Encapsulation.debagify(inPath);
            return false;
        }
        sendUpdateInfoMessage(seqId, sampleId, reqId, step, String.format("Reverting bagging on directory [%s]",
                inFile.getAbsolutePath()));
        Encapsulation.debagify(inPath);
        String toUpload = boxed.getAbsolutePath();
        boolean success = false;
        TransferManager manager = null;
        logger.trace("Building TransferManager");
        try {
            manager = TransferManagerBuilder.standard()
                    .withS3Client(conn)
                    .withMultipartUploadThreshold(1024L * 1024L * partSize)
                    .withMinimumUploadPartSize(1024L * 1024L * partSize)
                    .build();
            inPath = Paths.get(toUpload).toString();
            logger.trace("New inFile: {}", inFile);
            logger.trace("file.seperator: {}", File.separatorChar);
            s3Prefix += inPath.substring((inPath.lastIndexOf(File.separatorChar) > -1 ?
                    inPath.lastIndexOf(File.separatorChar) + 1 : 0));

            logger.trace("s3Prefix: {}", s3Prefix);
            if (conn.doesObjectExist(bucket, s3Prefix)) {
                sendUpdateInfoMessage(seqId, sampleId, reqId, step, String.format("[%s] already contains the object [%s]",
                        bucket, s3Prefix));
                S3Object existingObject = conn.getObject(bucket, s3Prefix);
                String s3PrefixRename = s3Prefix + "." + new SimpleDateFormat("yyyy-MM-dd.HH-mm-ss-SSS")
                        .format(existingObject.getObjectMetadata().getLastModified());
                if (!removeExisting) {
                    sendUpdateInfoMessage(seqId, sampleId, reqId, step, String.format("[%s/%s] being renamed to [%s/%s]",
                            bucket, s3Prefix, bucket, s3PrefixRename));
                    if (existingObject.getObjectMetadata().getContentLength() > manager.getConfiguration().getMultipartCopyThreshold()) {
                        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucket,
                                s3PrefixRename);
                        InitiateMultipartUploadResult initResult = conn.initiateMultipartUpload(initRequest);
                        long objectSize = existingObject.getObjectMetadata().getContentLength();
                        long partsize = 1024 * 1024 * partSize;
                        long bytePosition = 0;
                        int partNum = 1;
                        int numParts = (int)(objectSize / partsize);
                        int notificationStep = 5;
                        int percentDone = 0;
                        int nextPercent = percentDone + notificationStep;
                        List<CopyPartResult> copyResponses = new ArrayList<>();
                        sendUpdateInfoMessage(seqId, sampleId, reqId, step,
                                String.format("Starting multipart upload (%d parts) to copy [%s/%s] to [%s/%s]",
                                        numParts, bucket, s3Prefix, bucket, s3PrefixRename));
                        while (bytePosition < objectSize) {
                            if ((partNum/numParts) > nextPercent) {
                                sendUpdateInfoMessage(seqId, sampleId, reqId, step,
                                        String.format("Copying in progress (%d/%d %d%%)",
                                                partNum, numParts, (partNum/numParts)));
                                nextPercent = percentDone + notificationStep;
                            }
                            long lastByte = Math.min(bytePosition + partSize - 1, objectSize - 1);
                            CopyPartRequest copyRequest = new CopyPartRequest()
                                    .withSourceBucketName(bucket)
                                    .withSourceKey(s3Prefix)
                                    .withDestinationBucketName(bucket)
                                    .withDestinationKey(s3PrefixRename)
                                    .withUploadId(initResult.getUploadId())
                                    .withFirstByte(bytePosition)
                                    .withLastByte(lastByte)
                                    .withPartNumber(partNum++);
                            copyResponses.add(conn.copyPart(copyRequest));
                            bytePosition += partSize;
                        }
                        logger.trace("Creating multipart upload completion request");
                        CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
                                bucket, s3PrefixRename, initResult.getUploadId(), getETags(copyResponses)
                        );
                        logger.trace("Completing multipart upload");
                        conn.completeMultipartUpload(completeRequest);
                    } else {
                        CopyObjectRequest copyObjectRequest = new CopyObjectRequest(bucket, s3Prefix, bucket,
                                s3PrefixRename);
                        conn.copyObject(copyObjectRequest);
                    }
                    sendUpdateInfoMessage(seqId, sampleId, reqId, step,
                            String.format("Existing object [%s/%s] copied successfully to [%s/%s]",
                                    bucket, s3Prefix, bucket, s3PrefixRename));
                }
                sendUpdateInfoMessage(seqId, sampleId, reqId, step,
                        String.format("Deleting existing object [%s/%s]", bucket, s3Prefix));
                DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucket, s3Prefix);
                conn.deleteObject(deleteObjectRequest);
                logger.trace("[{}] is ready for upload of [{}]", bucket, s3Prefix);
            }
            sendUpdateInfoMessage(seqId, sampleId, reqId, step,
                    String.format("Starting Upload to S3: [%s] => [%s/%s]",
                            boxed.getAbsolutePath(), bucket, s3Prefix));
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.addUserMetadata("uncompressedSize", String.valueOf(uncompressedSize));
            metadata.addUserMetadata("partSize", String.valueOf(partSize));
            PutObjectRequest request = new PutObjectRequest(bucket, s3Prefix, boxed).withMetadata(metadata);
            request.setGeneralProgressListener(new LoggingProgressListener(seqId, sampleId, reqId, step, boxed.length()));
            logger.trace("Starting upload to S3");
            long uploadStartTime = System.currentTimeMillis();
            Upload transfer = manager.upload(request);
            UploadResult result = transfer.waitForUploadResult();
            long uploadEndTime = System.currentTimeMillis();
            Duration uploadDuration = Duration.of(uploadEndTime - uploadStartTime, ChronoUnit.MILLIS);
            logger.trace("Upload finished in {}", formatDuration(uploadDuration));
            String s3Checksum = result.getETag();
            logger.trace("s3Checksum: {}", result.getETag());
            String localChecksum;
            if (s3Checksum.contains("-"))
                localChecksum = md5t.getMultiCheckSum(toUpload, manager);
            else
                localChecksum = md5t.getCheckSum(toUpload);
            logger.trace("localChecksum: {}", localChecksum);
            if (!localChecksum.equals(result.getETag()))
                sendUpdateErrorMessage(seqId, sampleId, reqId, step,
                        String.format("Checksums don't match [local: %s, S3: %s]", localChecksum, result.getETag()));
            Files.delete(boxed.toPath());
            success = localChecksum.equals(result.getETag());
        } catch (AmazonServiceException ase) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, step,
                    String.format("Caught an AmazonServiceException, which means your request made it "
                                    + "to Amazon S3, but was rejected with an error response for some reason. (" +
                                    "Error Message: %s, HTTP Status Code: %d, AWS Error Code: %s, Error Type: %s, " +
                                    "Request ID: %s",
                            ase.getMessage(), ase.getStatusCode(), ase.getErrorCode(),
                            ase.getErrorType().toString(), ase.getRequestId()));
        } catch (SdkClientException ace) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, step,
                    String.format("Caught an AmazonClientException, which means the client encountered "
                                    + "a serious internal problem while trying to communicate with S3, (" +
                                    "Error Message: %s",
                            ace.getMessage()));
        } catch (InterruptedException | IOException ie) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, step,
                    String.format("%s:%s", ie.getClass().getCanonicalName(), ie.getMessage()));
        } finally {
            try {
                assert manager != null;
                manager.shutdownNow();
            } catch (AssertionError ae) {
                sendUpdateErrorMessage(seqId, sampleId, reqId, step,
                        "uploadFile : TransferManager was pre-emptively shut down.");
            }
        }
        return success;
    }

    public boolean downloadBaggedDirectory(String bucket, String s3Prefix, String destinationDirectory,
                                           String seqId, String sampleId, String reqId, String step) {
        logger.debug("downloadBaggedDirectory('{}','{}','{}','{}','{}','{}','{}')", bucket, s3Prefix,
                destinationDirectory, seqId, sampleId, reqId, step);
        if (!conn.doesBucketExistV2(bucket)) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, step, String.format("Bucket [%s] does not exist", bucket));
            return false;
        }
        String objectToDownload = s3Prefix + extension;
        if (!conn.doesObjectExist(bucket, objectToDownload)) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, step,
                    String.format("Bucket [%s] does not contain [%s]", bucket, objectToDownload));
            return false;
        }
        S3Object s3Object = conn.getObject(bucket, objectToDownload);
        long s3ObjectSize = s3Object.getObjectMetadata().getContentLength();
        if (!destinationDirectory.endsWith("/"))
            destinationDirectory += "/";
        File downloadDir = new File(destinationDirectory);
        logger.trace("downloadDir.getAbsolutePath(): {}", downloadDir.getAbsolutePath());
        if (!downloadDir.exists()) {
            if (!downloadDir.mkdirs()) {
                sendUpdateErrorMessage(seqId, sampleId, reqId, step,
                        String.format("Output directory [%s] does not exist and could not be created",
                                downloadDir.getAbsolutePath()));
                return false;
            }
        }
        boolean success = false;

        TransferManager manager = null;
        logger.debug("Building TransferManager");
        try {
            String s3Checksum = s3Object.getObjectMetadata().getETag();
            logger.trace("s3Checksum: {}", s3Checksum);
            manager = TransferManagerBuilder.standard()
                    .withS3Client(conn)
                    .withMultipartUploadThreshold(1024L * 1024L * partSize)
                    .withMinimumUploadPartSize(1024L * 1024L * partSize)
                    .build();
            String outFileName = destinationDirectory + objectToDownload.substring(
                    objectToDownload.lastIndexOf("/") > -1 ? objectToDownload.lastIndexOf("/") : 0);
            logger.trace("outFileName: {}", outFileName);
            File outFile = new File(outFileName);
            logger.trace("outFile.getAbsolutePath(): {}", outFile.getAbsolutePath());
            GetObjectRequest request = new GetObjectRequest(bucket, objectToDownload);
            request.setGeneralProgressListener(new LoggingProgressListener(seqId, sampleId, reqId, step,
                    s3Object.getObjectMetadata().getContentLength()));
            sendUpdateInfoMessage(seqId, sampleId, reqId, step,
                    String.format("Initiating download: [%s] => [%s]", objectToDownload, outFile.getAbsolutePath()));
            Download transfer = manager.download(request, outFile);
            transfer.waitForCompletion();
            if (!outFile.exists()) {
                sendUpdateErrorMessage(seqId, sampleId, reqId, step,
                        String.format("[%s] does not exist after download of [%s]",
                                outFile.getAbsolutePath(), objectToDownload));
                return false;
            }
            sendUpdateInfoMessage(seqId, sampleId, reqId, step,
                    String.format("Verifying download via checksums",
                            objectToDownload, outFile.getAbsolutePath()));
            String localChecksum;
            if (s3Checksum.contains("-"))
                localChecksum = md5t.getMultiCheckSum(outFile.getAbsolutePath(), manager);
            else
                localChecksum = md5t.getCheckSum(outFile.getAbsolutePath());
            logger.debug("localChecksum: {}", localChecksum);
            if (!localChecksum.equals(s3Checksum))
                sendUpdateErrorMessage(seqId, sampleId, reqId, step,
                        String.format("Checksums don't match [local: %s, S3: %s]", localChecksum, s3Checksum));
            success = localChecksum.equals(s3Checksum);
        } catch (AmazonServiceException ase) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, step,
                    String.format("Caught an AmazonServiceException, which means your request made it "
                                    + "to Amazon S3, but was rejected with an error response for some reason. (" +
                                    "Error Message: %s, HTTP Status Code: %d, AWS Error Code: %s, Error Type: %s, " +
                                    "Request ID: %s",
                            ase.getMessage(), ase.getStatusCode(), ase.getErrorCode(),
                            ase.getErrorType().toString(), ase.getRequestId()));
        } catch (SdkClientException ace) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, step,
                    String.format("Caught an AmazonClientException, which means the client encountered "
                                    + "a serious internal problem while trying to communicate with S3, (" +
                                    "Error Message: %s",
                            ace.getMessage()));
        } catch (InterruptedException | IOException ie) {
            sendUpdateErrorMessage(seqId, sampleId, reqId, step,
                    String.format("%s:%s", ie.getClass().getCanonicalName(), ie.getMessage()));
        } finally {
            try {
                assert manager != null;
                manager.shutdownNow();
            } catch (AssertionError ae) {
                sendUpdateErrorMessage(seqId, sampleId, reqId, step,
                        "uploadFile : TransferManager was pre-emptively shut down.");
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
            tx = TransferManagerBuilder.standard()
                    .withS3Client(conn)
                    .withMultipartUploadThreshold(1024L * 1024L * partSize)
                    .withMinimumUploadPartSize(1024L * 1024L * partSize)
                    .build();

            /*logger.trace("Building new TransferManagerConfiguration");
            TransferManagerConfiguration tmConfig = new TransferManagerConfiguration();
            logger.trace("Setting up minimum part size");

            // Sets the minimum part size for upload parts.
            tmConfig.setMinimumUploadPartSize(partSize * 1024 * 1024);
            logger.trace("Setting up size threshold for multipart uploads");
            // Sets the size threshold in bytes for when to use multipart uploads.
            tmConfig.setMultipartUploadThreshold((long) partSize * 1024 * 1024);
            logger.trace("Setting configuration on TransferManager");
            tx.setConfiguration(tmConfig);*/

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
            tx = TransferManagerBuilder.standard()
                    .withS3Client(conn)
                    .withMultipartUploadThreshold(1024L * 1024L * partSize)
                    .withMinimumUploadPartSize(1024L * 1024L * partSize)
                    .build();

            /*logger.trace("Building new TransferManagerConfiguration");
            TransferManagerConfiguration tmConfig = new TransferManagerConfiguration();
            logger.trace("Setting up minimum part size");

            // Sets the minimum part size for upload parts.
            tmConfig.setMinimumUploadPartSize(partSize * 1024 * 1024);
            logger.trace("Setting up size threshold for multipart uploads");
            // Sets the size threshold in bytes for when to use multipart uploads.
            tmConfig.setMultipartUploadThreshold((long) partSize * 1024 * 1024);
            logger.trace("Setting configuration on TransferManager");
            tx.setConfiguration(tmConfig);*/

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
            tx = TransferManagerBuilder.standard()
                    .withS3Client(conn)
                    .withMultipartUploadThreshold(1024L * 1024L * partSize)
                    .withMinimumUploadPartSize(1024L * 1024L * partSize)
                    .build();

            /*logger.trace("Building new TransferManagerConfiguration");
            TransferManagerConfiguration tmConfig = new TransferManagerConfiguration();
            logger.trace("Setting up minimum part size");

            // Sets the minimum part size for upload parts.
            tmConfig.setMinimumUploadPartSize(partSize * 1024 * 1024);
            logger.trace("Setting up size threshold for multipart uploads");
            // Sets the size threshold in bytes for when to use multipart uploads.
            tmConfig.setMultipartUploadThreshold((long) partSize * 1024 * 1024);
            logger.trace("Setting configuration on TransferManager");
            tx.setConfiguration(tmConfig);*/

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
                tx = TransferManagerBuilder.standard()
                        .withS3Client(conn)
                        .withMultipartUploadThreshold(1024L * 1024L * partSize)
                        .withMinimumUploadPartSize(1024L * 1024L * partSize)
                        .build();

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

    public Map<String, Long> getlistBucketContents(String bucket, String prefixKey) {
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
        return conn.doesBucketExistV2(bucket);
    }

    public void createBucket(String bucket) {
        logger.debug("Call to createBucket [bucket = {}]", bucket);
        try {
            if (!conn.doesBucketExistV2(bucket)) {
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
        private long lastTimestamp = 0L;
        private long totalTransferred = 0L;
        private long lastTransferred = 0L;
        private long totalBytes = 0L;
        private int nextUpdate = updatePercentStep;
        private String seqId;
        private String sampleId;
        private String reqId;
        private String step;

        public LoggingProgressListener(String seqId, String sampleId, String reqId, String step, long totalBytes) {
            this.startTimestamp = System.currentTimeMillis();
            this.lastTimestamp = startTimestamp;
            this.seqId = seqId;
            this.sampleId = sampleId;
            this.reqId = reqId;
            this.step = step;
            this.totalBytes = totalBytes;
        }

        @Override
        public void progressChanged(ProgressEvent progressEvent) {
            Thread.currentThread().setName("TransferListener");
            long currentBytesTransferred = progressEvent.getBytesTransferred();
            this.totalTransferred += currentBytesTransferred;
            this.lastTransferred += currentBytesTransferred;
            float currentTransferPercentage = ((float)totalTransferred / (float)totalBytes) * (float)100;
            if (currentTransferPercentage > (float)nextUpdate) {
                long currentTimestamp = System.currentTimeMillis();
                sendUpdateInfoMessage(seqId, sampleId, reqId, step,
                        String.format("Transferring (%s/%s %d%%) at %s",
                        humanReadableByteCount(totalTransferred, true), humanReadableByteCount(totalBytes, true),
                        (int)currentTransferPercentage,
                                humanReadableTransferRate(lastTransferred, currentTimestamp - lastTimestamp)));
                lastTransferred = 0L;
                lastTimestamp = currentTimestamp;
                nextUpdate += updatePercentStep;
            }
        }
    }

    private void sendUpdateInfoMessage(String seqId, String sampleId, String reqId, String step, String message) {
        logger.info("{}", message);
        MsgEvent msgEvent = plugin.genGMessage(MsgEvent.Type.INFO, message);
        msgEvent.setParam("pathstage", String.valueOf(plugin.pathStage));
        msgEvent.setParam("seq_id", seqId);
        if (sampleId != null) {
            msgEvent.setParam("sample_id", sampleId);
            msgEvent.setParam("ssstep", step);
        } else
            msgEvent.setParam("sstep", step);
        if (reqId != null)
            msgEvent.setParam("req_id", reqId);
        plugin.sendMsgEvent(msgEvent);
    }

    private void sendUpdateErrorMessage(String seqId, String sampleId, String reqId, String step, String message) {
        logger.error("{}", message);
        MsgEvent msgEvent = plugin.genGMessage(MsgEvent.Type.ERROR, message);
        msgEvent.setParam("pathstage", String.valueOf(plugin.pathStage));
        msgEvent.setParam("error_message", message);
        msgEvent.setParam("seq_id", seqId);
        if (sampleId != null) {
            msgEvent.setParam("sample_id", sampleId);
            msgEvent.setParam("ssstep", step);
        } else
            msgEvent.setParam("sstep", step);
        if (reqId != null)
            msgEvent.setParam("req_id", reqId);
        plugin.sendMsgEvent(msgEvent);
    }

    // This is a helper function to construct a list of ETags.
    private static List<PartETag> getETags(List<CopyPartResult> responses) {
        List<PartETag> etags = new ArrayList<>();
        for (CopyPartResult response : responses) {
            etags.add(new PartETag(response.getPartNumber(), response.getETag()));
        }
        return etags;
    }

    private static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    private static String humanReadableTransferRate(long transferred, long duration) {
        float rate = (((float)transferred * (float)1000) * (float)8) / duration;
        int unit = 1000;
        if ((int)rate < unit) return String.format("%.1f bps", rate);
        int exp = (int) (Math.log(rate) / Math.log(unit));
        String pre = "kMGTPE".charAt(exp - 1) + "";
        return String.format("%.1f %sb/s", rate / Math.pow(unit, exp), pre);
    }

    private static String formatDuration(Duration duration) {
        long seconds = duration.getSeconds();
        long absSeconds = Math.abs(seconds);
        String positive = String.format(
                "%d:%02d:%02d",
                absSeconds / 3600,
                (absSeconds % 3600) / 60,
                absSeconds % 60);
        return seconds < 0 ? "-" + positive : positive;
    }
}