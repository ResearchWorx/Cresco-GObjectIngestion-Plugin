package com.researchworx.cresco.plugins.gobjectIngestion.objectstorage;

//import java.io.ByteArrayInputStream;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.TransferManager;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

//import java.io.InputStream;
//import com.amazonaws.services.s3.model.ObjectMetadata;
//import com.amazonaws.services.s3.model.PutObjectRequest;

public class ObjectEngine_new {
    //private static final Logger logger = LoggerFactory.getLogger(ObjectEngine.class);
    private static AmazonS3 conn;
    //private final static String FOLDER_SUFFIX = "/";
    private int partSize;

    public ObjectEngine_new() {
        //this.logger = new CLogger(plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID());

        System.out.println("Building AWS Credentials");
        AWSCredentials credentials = new BasicAWSCredentials("TAWYFO3X05X80BMH337I", "n59CneIRx7vp23rnLvHP7yst70dITEUufk+3yVyZ");

        System.out.println("Building ClientConfiguration");
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setProtocol(Protocol.HTTPS);
        clientConfig.setSignerOverride("S3SignerType");
        //clientConfig.setMaxConnections(100);


        System.out.println("Connecting to Amazon S3");
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



        //System.out.println("Building new MD5Tools");
        //md5t = new MD5Tools(plugin);

    }

    private static boolean createDir(File file) {
        boolean isCreated = false;
        try{
            //File file = new File(directories);

            // The mkdirs will create folder including any necessary but non existence
            // parent directories. This method returns true if and only if the directory
            // was created along with all necessary parent directories.
            isCreated = file.mkdirs();

        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        return isCreated;
    }

    private class DownloadWorker implements Runnable {
        private TransferManager tx;
        private String keyPrefix;
        private String bucketName;
        private File file;

        public DownloadWorker(TransferManager tx, String bucketName, String keyPrefix, File file) {
            this.tx = tx;
            this.keyPrefix = keyPrefix;
            this.bucketName = bucketName;
            this.file = file;
        }

        @Override
        public void run() {

            try {


                Download dl = tx.download(bucketName, keyPrefix, file);

                /*
                while (!dl.isDone()) {
                //System.out.println(Thread.currentThread().getId() + " Downloading: " + keyPrefix);
                   Thread.sleep(10);
                }
                */

                /*
                S3Object object = conn.getObject(
                        new GetObjectRequest(bucketName, keyPrefix));
                InputStream objectData = object.getObjectContent();

                byte[] buffer = new byte[objectData.available()];
                objectData.read(buffer);

                OutputStream outStream = new FileOutputStream(file);
                outStream.write(buffer);

                outStream.close();
                objectData.close();
                object.close();
                */
            }
            catch(Exception ex) {
                ex.printStackTrace();
            }

        }

    }

    //	downloadDirectory(String bucketName, String keyPrefix, File destinationDirectory)
    public boolean downloadDirectoryLarge(String bucketName, String keyPrefix, String destinationDirectory) {
        //System.out.println("Call to downloadDirectory [bucketName = {}, keyPrefix = {}, destinationDirectory = {}", bucketName, keyPrefix, destinationDirectory);
        boolean wasTransfered = false;
        TransferManager tx = null;


        try {
            if(!destinationDirectory.endsWith("/")) {
                destinationDirectory += "/";
            }

            System.out.println("Building new TransferManager");
            tx = new TransferManager(conn);

            ExecutorService downloadExecutorService = Executors.newFixedThreadPool(5);
            System.out.println("Building Structure [downloadDir] to [desinationDirectory]");
            Download dl = null;
            List<String> dirList =  getlistBucketContents(bucketName, keyPrefix);
            for(String dir : dirList) {
                String tdir = dir.substring(0,dir.lastIndexOf("/"));
                File directory = new File(destinationDirectory + tdir);
                File file = new File(destinationDirectory + dir);


                if(!directory.exists())
                {
                    if(createDir(directory)) {
                        System.out.println("created directory : " + directory.getAbsolutePath());
                    }
                    else {
                        System.out.println("failed creating directory : " + directory.getAbsolutePath());
                    }
                }
                downloadExecutorService.execute(new DownloadWorker(tx,bucketName,dir,file));
                /*
                dl = tx.download(bucketName, keyPrefix, file);
                while (!dl.isDone()) {
                    //System.out.println(Thread.currentThread().getId() + " Downloading: " + keyPrefix);
                    Thread.sleep(1000);
                }
                */
            }


            System.out.println("Downloading [downloadDir] to [desinationDirectory]");


            downloadExecutorService.shutdown();
            downloadExecutorService.awaitTermination(10, TimeUnit.DAYS);

            wasTransfered = true;

        } catch (Exception ex) {
            //logger.error("downloadDirectory {}", ex.getMessage());
        } finally {
            try {
                assert tx != null;
                tx.shutdownNow();
            } catch (AssertionError e) {
                //logger.error("downloadDirectory - TransferManager was pre-emptively shutdown");
            }
        }
        return wasTransfered;
    }

    //	downloadDirectory(String bucketName, String keyPrefix, File destinationDirectory)
    public boolean downloadDirectory(String bucketName, String keyPrefix, String destinationDirectory) {
        //System.out.println("Call to downloadDirectory [bucketName = {}, keyPrefix = {}, destinationDirectory = {}", bucketName, keyPrefix, destinationDirectory);
        boolean wasTransfered = false;
        TransferManager tx = null;


        try {
            System.out.println("Building new TransferManager");
            tx = new TransferManager(conn);

            System.out.println("Setting [downloadDir] to [desinationDirectory]");
            File downloadDir = new File(destinationDirectory);
            if (!downloadDir.exists()) {
                if (!downloadDir.mkdirs()) {
                    //logger.error("Failed to create download directory!");
                    return false;
                }
            }

            System.out.println("Starting download timer");
            long startDownload = System.currentTimeMillis();
            //logger.info("Beginning download [bucketName = {}, keyPrefix = {}, downloadDir = {}", bucketName, keyPrefix, downloadDir);

            MultipleFileDownload myDownload = tx.downloadDirectory(bucketName, keyPrefix, downloadDir);

            //logger.info("Downloading: " + bucketName + ":" + keyPrefix + " to " + downloadDir);

            /*
			myDownload.addProgressListener(new ProgressListener() {
				// This method is called periodically as your transfer progresses
				public void progressChanged(ProgressEvent progressEvent) {
                    //System.out.println(myDownload.getProgress().getPercentTransferred());

                    //System.out.println(myDownload.getProgress().getPercentTransferred() + "%");
					System.out.println(progressEvent.getEventType());
					//System.out.println(progressEvent.getEventCode());
					//System.out.println(ProgressEvent.COMPLETED_EVENT_CODE);
					//if (progressEvent.getEventCode() == ProgressEvent.COMPLETED_EVENT_CODE) {
					//	System.out.println("download complete!!!");
					//}
				}
			});
			myDownload.waitForCompletion();
            */

            System.out.println("Entering download wait loop");

            while (!myDownload.isDone()) {
                System.out.println(myDownload.getProgress().getPercentTransferred() + "%");
                Thread.sleep(5000);
            }

            System.out.println("Calculating download statistics");
            float transferTime = (System.currentTimeMillis() - startDownload) / 1000;
            long bytesTransfered = myDownload.getProgress().getBytesTransferred();
            float transferRate = (bytesTransfered / 1000000) / transferTime;

            System.out.println("Download Transfer Desc: " + myDownload.getDescription());
            System.out.println("\t- Transfered : " + myDownload.getProgress().getBytesTransferred() + " bytes");
            System.out.println("\t- Elapsed time : " + transferTime + " seconds");
            System.out.println("\t- Transfer rate : " + transferRate + " MB/sec");


            // Transfers also allow you to set a <code>ProgressListener</code> to receive
            // asynchronous notifications about your transfer's progress.
            //myUpload.addProgressListener(myProgressListener);

            // Or you can block the current thread and wait for your transfer to
            // to complete. If the transfer fails, this method will throw an
            // AmazonClientException or AmazonServiceException detailing the reason.
            //myUpload.waitForCompletion();

            // After the upload is complete, call shutdownNow to release the resources.


            wasTransfered = true;

        } catch (Exception ex) {
            //logger.error("downloadDirectory {}", ex.getMessage());
        } finally {
            try {
                assert tx != null;
                tx.shutdownNow();
            } catch (AssertionError e) {
                //logger.error("downloadDirectory - TransferManager was pre-emptively shutdown");
            }
        }
        return wasTransfered;
    }


    public List<String> listBucketDirs(String bucket) {
        //System.out.println("Call to listBucketDirs [bucket = {}]", bucket);
        List<String> dirList = new ArrayList<>();
        try {
            if(doesBucketExist(bucket)) {
            System.out.println("Instantiating new ListObjectsRequest");
            ListObjectsRequest lor = new ListObjectsRequest();
            lor.setBucketName(bucket);
            lor.setDelimiter("/");

            System.out.println("Grabbing [objects] list from [lor]");
            //if(doesBucketExist(bucket))
            ObjectListing objects = conn.listObjects(lor);
            do {
                List<String> sublist = objects.getCommonPrefixes();
                System.out.println("Adding all Common Prefixes from [objects]");
                dirList.addAll(sublist);
                System.out.println("Grabbing next batch of [objects]");
                objects = conn.listNextBatchOfObjects(objects);
            } while (objects.isTruncated());
        }
            else {
                System.out.println("Bucket :" + bucket + " does not exist!");
                //logger.warn("Bucket :" + bucket + " does not exist!");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            //logger.error("listBucketDirs {}", ex.getMessage());
            dirList = null;
        }
        return dirList;
    }

    public List<String> getlistBucketContents(String bucket, String prefixKey) {
        //System.out.println("Call to listBucketContents [bucket = {}, searchName = {}]", bucket, searchName);
        List<String> dirList = new ArrayList<>();
        try {
            if(doesBucketExist(bucket)) {
                System.out.println("Grabbing [objects] list from [bucket]");
                ObjectListing objects = conn.listObjects(bucket);

                objects.setPrefix(prefixKey);
                do {
                    for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                        dirList.add(objectSummary.getKey() );
                    }
                    objects = conn.listNextBatchOfObjects(objects);
                } while (objects.isTruncated());
            }
            else{
                //logger.warn("Bucket :" + bucket + " does not exist!");
                System.out.println("Bucket :" + bucket + " does not exist!");
            }
        } catch (Exception ex) {
            //logger.error("listBucketContents {}", ex.getMessage());

            ex.printStackTrace();
        }
        return dirList;
    }

    public void listBucketContents(String bucket, String prefixKey, String searchName) {
        //System.out.println("Call to listBucketContents [bucket = {}, searchName = {}]", bucket, searchName);
        Map<String, String> fileMap = new HashMap<>();
        try {
            if(doesBucketExist(bucket)) {
                System.out.println("Grabbing [objects] list from [bucket]");
                ObjectListing objects = conn.listObjects(bucket);
                objects.setPrefix(prefixKey);
                do {
                    for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {

                        if(searchName != null) {
                            if (objectSummary.getKey().contains(searchName)) {
                                System.out.println(objectSummary.getKey() + " " + objectSummary.getSize() + " " + objectSummary.getETag());
                                //System.out.println("Found object {}\t{}\t{}\t{}", objectSummary.getKey(),
                                //        objectSummary.getSize(),
                                //        objectSummary.getETag(),
                                //        StringUtils.fromDate(objectSummary.getLastModified()));
                                //fileMap.put(objectSummary.getKey(), objectSummary.getETag());
                            }
                        }
                        else {
                            if(objectSummary.getKey().endsWith("/")) {
                                System.out.println(objectSummary.getKey() + " " + objectSummary.getSize() + " " + objectSummary.getETag());
                            }
                        }
                    }
                    objects = conn.listNextBatchOfObjects(objects);
                } while (objects.isTruncated());
            }
            else{
                //logger.warn("Bucket :" + bucket + " does not exist!");
                System.out.println("Bucket :" + bucket + " does not exist!");
            }
        } catch (Exception ex) {
            //logger.error("listBucketContents {}", ex.getMessage());
            fileMap = null;
            ex.printStackTrace();
        }

    }
    /*
    public Map<String, String> listBucketContents(String bucket, String searchName) {
        //System.out.println("Call to listBucketContents [bucket = {}, searchName = {}]", bucket, searchName);
        Map<String, String> fileMap = new HashMap<>();
        try {
            if(doesBucketExist(bucket)) {
            System.out.println("Grabbing [objects] list from [bucket]");
            ObjectListing objects = conn.listObjects(bucket);
            do {
                for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                    if (objectSummary.getKey().contains(searchName)) {
                        //System.out.println("Found object {}\t{}\t{}\t{}", objectSummary.getKey(),
                        //        objectSummary.getSize(),
                        //        objectSummary.getETag(),
                        //        StringUtils.fromDate(objectSummary.getLastModified()));
                        //fileMap.put(objectSummary.getKey(), objectSummary.getETag());
                    }
                }
                objects = conn.listNextBatchOfObjects(objects);
            } while (objects.isTruncated());
        }
            else{
                //logger.warn("Bucket :" + bucket + " does not exist!");
            }
        } catch (Exception ex) {
            //logger.error("listBucketContents {}", ex.getMessage());
            fileMap = null;
        }
        return fileMap;
    }
    */
    public boolean doesObjectExist(String bucket, String objectName) {
        //System.out.println("Call to doesObjectExist [bucket = {}, objectName = {}]", bucket, objectName);
        return conn.doesObjectExist(bucket, objectName);
    }

    public boolean doesBucketExist(String bucket) {
        //System.out.println("Call to doesBucketExist [bucket = {}]", bucket);
        return conn.doesBucketExist(bucket);
    }

    public void createBucket(String bucket) {
        //System.out.println("Call to createBucket [bucket = {}]", bucket);
        try {
            if (!conn.doesBucketExist(bucket)) {
                Bucket mybucket = conn.createBucket(bucket);
                //System.out.println("Created bucket [{}] ", bucket);
            }
        } catch (Exception ex) {
            //logger.error("createBucket {}", ex.getMessage());
        }

    }

    public void deleteBucketContents(String bucket) {
        //System.out.println("Call to deleteBucketContents [bucket = {}]", bucket);
        //System.out.println("Grabbing [objects] list from [bucket]");
        ObjectListing objects = conn.listObjects(bucket);
        do {
            for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                //System.out.println("Deleting [{}] object from [{}] bucket", objectSummary.getKey(), bucket);
                conn.deleteObject(bucket, objectSummary.getKey());

                //System.out.println("Deleted {}\t{}\t{}\t{}", objectSummary.getKey(),
                //        objectSummary.getSize(),
                //        objectSummary.getETag(),
                //        StringUtils.fromDate(objectSummary.getLastModified()));
            }
            objects = conn.listNextBatchOfObjects(objects);
        } while (objects.isTruncated());
    }
}
