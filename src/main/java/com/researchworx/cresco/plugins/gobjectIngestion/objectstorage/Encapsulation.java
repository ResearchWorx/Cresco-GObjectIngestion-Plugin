package com.researchworx.cresco.plugins.gobjectIngestion.objectstorage;

import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.plugin.core.CPlugin;
import com.researchworx.cresco.library.utilities.CLogger;
import gov.loc.repository.bagit.creator.BagCreator;
import gov.loc.repository.bagit.domain.Bag;
import gov.loc.repository.bagit.exceptions.*;
import gov.loc.repository.bagit.hash.StandardSupportedAlgorithms;
import gov.loc.repository.bagit.hash.SupportedAlgorithm;
import gov.loc.repository.bagit.reader.BagReader;
import net.java.truevfs.access.TArchiveDetector;
import net.java.truevfs.access.TConfig;
import net.java.truevfs.access.TFile;
import net.java.truevfs.access.TVFS;
import net.java.truevfs.comp.tardriver.TarDriver;
import net.java.truevfs.comp.zipdriver.JarDriver;
import net.java.truevfs.comp.zipdriver.ZipDriver;
import net.java.truevfs.driver.tar.bzip2.TarBZip2Driver;
import net.java.truevfs.driver.tar.gzip.TarGZipDriver;
import net.java.truevfs.driver.tar.xz.TarXZDriver;
import net.java.truevfs.kernel.spec.FsAccessOption;
import net.java.truevfs.kernel.spec.FsSyncException;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Encapsulation {
    private static CLogger logger = new CLogger(Encapsulation.class, new ConcurrentLinkedQueue<MsgEvent>(),
            "", "", "");
    private static final int max_batch_size = 1000;

    public static void setLogger(CPlugin plugin) {
        logger = new CLogger(Encapsulation.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(),
                plugin.getPluginID(), CLogger.Level.Trace);
    }

    /**
     * Function to encapusulate a directory into the selected BagIt and compression formats
     * @param src Directory to encapsulate
     * @param bagItMode Mode of BagIt to use (none, dotfile, standard)
     * @param hashing Hash method to use for BagIt verification
     * @param includeHiddenFiles Whether to include hidden files in the BagIt bag
     * @param boxItExtension Compression method to use (tar, bzip2, gzip, xz, zip)
     * @return The absolute file path to the resulting encapsulation (directory or compressed file)
     */
    static String encapsulate(String src, String bagItMode, String hashing,
                              boolean includeHiddenFiles, String boxItExtension) {
        logger.trace("Call to encapsulate({}, {}, {}, {}, {})",
                src != null ? src : "NULL", bagItMode != null ? bagItMode : "NULL",
                hashing != null ? hashing : "NULL", includeHiddenFiles,
                boxItExtension != null ? boxItExtension : "NULL");
        if (src == null || src.equals("")) {
            logger.error("No valid src given");
            return null;
        }
        if (bagItMode == null || bagItMode.equals("")) {
            logger.error("No valid bagItMode given");
            return null;
        }
        if (hashing == null || hashing.equals("")) {
            logger.error("No valid hashing given");
            return null;
        }
        if (boxItExtension == null || boxItExtension.equals("")) {
            logger.error("No valid boxItExtension given");
            return null;
        }
        File capsule = new File(src);
        if (!capsule.exists())
            return null;
        if (capsule.isFile())
            return src;
        if (!bagItMode.equals("none"))
            capsule = bagItUp(capsule, bagItMode, hashing, includeHiddenFiles);
        if (capsule == null) {
            logger.error("Failed to bag up [{}]", new File(src).getAbsolutePath());
            return null;
        }
        logger.trace("Path post bagItUp: {}", capsule.getAbsolutePath());
        if (!boxItExtension.equals("none"))
            capsule = boxItUp(capsule, boxItExtension);
        if (capsule == null) {
            logger.error("Failed to box up [{}]", new File(src).getAbsolutePath());
            return null;
        }
        logger.trace("Path post boxItUp: {}", capsule.getAbsolutePath());
        if (!boxItExtension.equals("none")) {
            debagify(src);
        }
        return capsule.getAbsolutePath();
    }

    /**
     * Function to restore an encapsulated file
     * @param src File to restore
     * @return The absolute file path to the restored
     */
    /*public static String restore(String src) {
        String unboxed = unBoxIt(src);
        if (unboxed == null)
            unboxed = src;
        else
            new File(src).delete();
        if (isBag(unboxed)) {
            if (!verifyBag(unboxed, true))
                return null;
            debagify(unboxed);
        }
        return unboxed;
    }*/

    public static boolean isBag(String src) {
        logger.trace("Call to isBag('{}')", src);
        File bag = new File(src);
        //logger.debug("bag.exists(): {}", bag.exists());
        if (!bag.exists())
            return false;
        //logger.debug("bag.isFile(): {}", bag.isFile());
        if (bag.isFile())
            return false;
        if (!src.endsWith("/"))
            src += "/";
        File bagitFile = new File(src + ".bagit");
        //logger.debug(".bagit.exists(): {}", bagitFile.exists());
        if (new File(src + ".bagit").exists())
            return true;
        File data = new File(src + "data");
        //logger.debug("{} : exists() = {}, isDirectory() = {}", data.getAbsolutePath(), data.exists(), data.isDirectory());
        boolean hasBagitTxt = new File(src + "bagit.txt").exists();
        //logger.debug("hasBagitTxt : {}", hasBagitTxt);
        boolean hasBagitInfo = new File(src + "bag-info.txt").exists();
        //logger.debug("hasBagitInfo : {}", hasBagitInfo);
        boolean manifestSHA512 = new File(src + "manifest-sha512.txt").exists();
        //logger.debug("manifestSHA512 : {}", manifestSHA512);
        boolean tagmanifestSHA512 = new File(src + "tagmanifest-sha512.txt").exists();
        //logger.debug("tagmanifestSHA512 : {}", tagmanifestSHA512);
        boolean hasSHA512 = manifestSHA512 && tagmanifestSHA512;
        //logger.debug("hasSHA512 : {}", hasSHA512);
        boolean manifestSHA256 = new File(src + "manifest-sha256.txt").exists();
        //logger.debug("hasSmanifestSHA256HA512 : {}", manifestSHA256);
        boolean tagmanifestSHA256 = new File(src + "tagmanifest-sha256.txt").exists();
        //logger.debug("tagmanifestSHA256 : {}", tagmanifestSHA256);
        boolean hasSHA256 = manifestSHA256 && tagmanifestSHA256;
        //logger.debug("hasSHA256 : {}", hasSHA256);
        boolean manifestSHA1 = new File(src + "manifest-sha1.txt").exists();
        //logger.debug("manifestSHA1 : {}", manifestSHA1);
        boolean tagmanifestSHA1 = new File(src + "tagmanifest-sha1.txt").exists();
        //logger.debug("tagmanifestSHA1 : {}", tagmanifestSHA1);
        boolean hasSHA1 = manifestSHA1 && tagmanifestSHA1;
        //logger.debug("hasSHA1 : {}", hasSHA1);
        boolean manifestMD5 = new File(src + "manifest-md5.txt").exists();
        //logger.debug("manifestMD5 : {}", manifestMD5);
        boolean tagmanifestMD5 = new File(src + "tagmanifest-md5.txt").exists();
        //logger.debug("tagmanifestMD5 : {}", tagmanifestMD5);
        boolean hasMD5 = manifestMD5 && tagmanifestMD5;
        //logger.debug("hasMD5 : {}", hasMD5);
        return (data.exists() && data.isDirectory() && hasBagitTxt && hasBagitInfo &&
                (hasSHA512 || hasSHA256 || hasSHA1 || hasMD5));
        //    return true;
        //return false;
    }

    /**
     * Build BagIt bag from directory
     * @param folder Directory to bag up
     * @param mode Mode of BagIt bag creation
     * @param hashing Hashing method to use for file verification
     * @param includeHiddenFiles Whether to include hidden files
     * @return The resulting bag path
     */
    public static File bagItUp(File folder, String mode, String hashing, boolean includeHiddenFiles) {
        logger.trace("Call to bagItUp({}, {}, {}, {})", folder.getAbsolutePath(), mode, hashing, includeHiddenFiles);
        if (isBag(folder.getAbsolutePath()))
            debagify(folder.getAbsolutePath());
        List<SupportedAlgorithm> algorithms = new ArrayList<>();
        if (hashing.equals("sha512"))
            algorithms.add(StandardSupportedAlgorithms.SHA512);
        if (hashing.equals("sha256"))
            algorithms.add(StandardSupportedAlgorithms.SHA256);
        if (hashing.equals("sha1"))
            algorithms.add(StandardSupportedAlgorithms.SHA1);
        if (hashing.equals("md5"))
            algorithms.add(StandardSupportedAlgorithms.MD5);
        switch (mode) {
            case "dotfile":
                try {
                    BagCreator.createDotBagit(folder.toPath(), algorithms, includeHiddenFiles);
                } catch (IOException e) {
                    logger.error("bagItUp : File error encountered while creating BagIt bag : {}, {}", folder.getAbsolutePath(), e.getMessage());
                    return null;
                } catch (NoSuchAlgorithmException e) {
                    logger.error("bagItUp : Unsupported algorithm selected.");
                    return null;
                }
                break;
            case "standard":
                try {
                    BagCreator.bagInPlace(folder.toPath(), algorithms, includeHiddenFiles);
                } catch (IOException e) {
                    logger.error("bagItUp : File error encountered while creating BagIt bag : {}, {}", folder.getAbsolutePath(), e.getMessage());
                    return null;
                } catch (NoSuchAlgorithmException e) {
                    logger.error("bagItUp : Unsupported algorithm selected.");
                    return null;
                }
                break;
        }
        if (verifyBag(folder, includeHiddenFiles))
            return folder;
        return null;
    }

    /**
     * Cleans up from the BagIt bag creation
     * @param src The path to the bag to clean up
     */
    public static void debagify(String src) {
        logger.trace("Call to debagify({})", src);
        File bag = new File(src);
        if (bag.isFile())
            return;
        File bagIt = new File(src + "/.bagit");
        if (bagIt.exists())
            try {
                deleteFolder(bagIt.toPath());
            } catch (IOException e) {
                logger.error("Failed to delete the .bagit directory for bag: {}", src);
            }
        new File(src + "/bagit.txt").delete();
        new File(src + "/bag-info.txt").delete();
        new File(src + "/manifest-sha512.txt").delete();
        new File(src + "/manifest-sha256.txt").delete();
        new File(src + "/manifest-sha1.txt").delete();
        new File(src + "/manifest-md5.txt").delete();
        new File(src + "/tagmanifest-sha512.txt").delete();
        new File(src + "/tagmanifest-sha256.txt").delete();
        new File(src + "/tagmanifest-sha1.txt").delete();
        new File(src + "/tagmanifest-md5.txt").delete();
        File data = new File(src + "/data");
        if (data.exists()) {
            String tmpDataPath = src + "/" + UUID.randomUUID().toString();
            data.renameTo(new File(tmpDataPath));
            try {
                copyFolderContents(new File(tmpDataPath), new File(src));
                deleteFolder(new File(tmpDataPath).toPath());
                new File(src + "/bag-info.txt").delete();
            } catch (IOException e) {
                logger.error("Failed to move files from {} to {}", src + "/data", src);
            }
        }
    }

    /**
     * Build a compressed file from the given directory
     * @param capsule Directory to compress
     * @param extension Compression method to use
     * @return Path to the compressed file
     */
    public static File boxItUp(File capsule, String extension) {
        logger.trace("Call to boxItUp({}, {})", capsule.getAbsolutePath(), extension);
        if (!capsule.exists())
            return null;
        TConfig.current().setLenient(false);
        TConfig.current().setArchiveDetector(new TArchiveDetector(TArchiveDetector.NULL, new Object[][] {
                { "jar", new JarDriver() },
                { "tar", new TarDriver() },
                { "tar.gz", new TarGZipDriver() },
                { "tar.xz", new TarXZDriver() },
                { "tar.bz2", new TarBZip2Driver() },
                { "zip", new ZipDriver() },
        }));
        TConfig.current().setAccessPreference(FsAccessOption.GROW, true);
        String archiveName = capsule.getAbsolutePath();
        switch (extension) {
            case "tar":
                archiveName = archiveName + ".tar";
                break;
            case "bzip2":
                archiveName = archiveName + ".tar.bz2";
                break;
            case "gzip":
                archiveName = archiveName + ".tar.gz";
                break;
            case "xz":
                archiveName = archiveName + ".tar.xz";
                break;
            case "zip":
                archiveName = archiveName + ".zip";
                break;
            default:
                return null;
        }
        TFile bag = new TFile(capsule);
        TFile archive;
        try {
            archive = new TFile(archiveName);
            if (archive.exists())
                archive.rm_r();
            archive.mkdir(false);
        } catch (IOException e) {
            logger.error("boxItUp : Failed to create archive directory. {}:{}", e.getClass().getName(), e.getMessage());
            return null;
        }
        if (TConfig.current().isLenient() && archive.isArchive() || archive.isDirectory())
            archive = new TFile(archive, bag.getName());
        try {
            bag.cp_rp(archive);
        } catch (IOException e) {
            logger.error("boxItUp : Failed to write files into archive: {}", e.getMessage());
            return null;
        }
        try {
            TVFS.umount();
        } catch (FsSyncException e) {
            logger.error("boxItUp : Failed to sync changes to the filesystem: ", e.getMessage());
            return null;
        }

        return new File(archiveName);
    }

    public static boolean decompress(String in, File out) {
        logger.trace("decompress('{}','{}')", in, out.getAbsolutePath());
        ExecutorService exec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        try (TarArchiveInputStream fin = new TarArchiveInputStream(new FileInputStream(in))){
            LinkedList<String> toExtract = new LinkedList<>();
            TarArchiveEntry entry;
            while ((entry = fin.getNextTarEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }
                toExtract.add(new File(out, entry.getName()).getAbsolutePath());
                //File curfile = new File(out, entry.getName());
                //logger.trace("Extracting [{}]", entry.getName());
                /*File parent = curfile.getParentFile();
                if (!parent.exists()) {
                    parent.mkdirs();
                }
                IOUtils.copy(fin, new FileOutputStream(curfile));*/
                //exec.execute(new DecompressWorker(curfile, fin));
            }
            while (toExtract.size() > 0) {
                HashSet<String> batchToExtract = new HashSet<>();
                while (batchToExtract.size() < max_batch_size && toExtract.size() > 0)
                    batchToExtract.add(toExtract.pop());
                final CountDownLatch latch = new CountDownLatch(batchToExtract.size());
                for (final String fileToExtract : batchToExtract) {
                    logger.trace("Extracting [{}]", fileToExtract);
                    exec.execute(new DecompressWorker(fileToExtract, fin, latch));
                }
                latch.await();
            }
            logger.trace("Extraction complete");
            exec.shutdownNow();
            return true;
        } catch (InterruptedException ie){
            logger.error("Failed to decompress [{}], interrupted", in);
            return false;
        } catch (FileNotFoundException fnfe) {
            logger.error("Failed to decompress [{}], file not found [{}:{}]",
                    in, fnfe.getClass().getCanonicalName(), fnfe.getMessage());
            return false;
        } catch (IOException ioe) {
            logger.error("Failed to decompress [{}], encountered I/O exception [{}:{}]",
                    in, ioe.getClass().getCanonicalName(), ioe.getMessage());
            return false;
        }
    }

    private static class DecompressWorker implements Runnable {
        private final TarArchiveInputStream fin;
        private final String curfile;
        private final CountDownLatch latch;
        DecompressWorker(String curfile, TarArchiveInputStream fin, CountDownLatch latch) {
            this.curfile = curfile;
            this.fin = fin;
            this.latch = latch;
        }
        @Override
        public void run() {
            try {
                if (curfile == null) {
                    logger.error("curfile cannot be null");
                    return;
                }
                if (fin == null) {
                    logger.error("fin cannot be null");
                    return;
                }
                File toExtract = new File(curfile);
                File parent = toExtract.getParentFile();
                if (!parent.exists()) {
                    parent.mkdirs();
                }
                FileOutputStream out = new FileOutputStream(toExtract);
                IOUtils.copy(fin, out);
                out.close();
                latch.countDown();
            } catch (FileNotFoundException fnfe) {
                logger.error("Failed to decompress [{}], file not found [{}:{}]",
                        curfile, fnfe.getClass().getCanonicalName(), fnfe.getMessage());
            } catch (IOException ioe) {
                logger.error("Failed to decompress [{}], encountered I/O exception [{}:{}]",
                        curfile, ioe.getClass().getCanonicalName(), ioe.getMessage());
            }
        }
    }

    public static boolean unBoxIt(String filePath) {
        logger.trace("unBoxIt('{}')", filePath);
        if (filePath == null || filePath.equals("")) {
            logger.error("Empty filepath given");
            return false;
        }
        logger.trace("Building TArchiveDetector");
        TConfig.current().setArchiveDetector(new TArchiveDetector(TArchiveDetector.NULL, new Object[][] {
                //{ "jar", new JarDriver() },
                { "tar", new TarDriver() },
                { "tar.gz", new TarGZipDriver() },
                { "tar.xz", new TarXZDriver() },
                { "tar.bz2", new TarBZip2Driver() },
                { "zip", new ZipDriver() },
        }));
        logger.trace("Setting TConfig preferences");
        TConfig.current().setAccessPreference(FsAccessOption.GROW, true);
        TConfig.current().setAccessPreference(FsAccessOption.STORE, true);
        TFile archive = new TFile(new File(filePath));
        logger.trace("Checking if [{}] exists", filePath);
        if (!archive.exists()) {
            logger.error("[{}] does not exist", archive.getAbsolutePath());
            return false;
        }
        logger.trace("Checking if [{}] is an archive", filePath);
        if (!archive.isArchive()) {
            logger.error("[{}] is not an archive", archive.getAbsolutePath());
            return false;
        }
        /*logger.trace("archive.canRead() = {}", archive.canRead());
        logger.trace("archive.canWrite() = {}", archive.canWrite());
        logger.trace("archive.canExecute() = {}", archive.canExecute());
        if (archive.getParentFile() == null) {
            logger.error("Failed to get parent folder of [{}]", archive.getAbsolutePath());
            return null;
        }
        logger.trace("archive.getParentFile().getAbsolutePath() = {}", archive.getParentFile().getAbsolutePath());
        String[] archiveFiles = archive.list();
        if (archiveFiles == null) {
            logger.error("Archive [{}] has a null file list", archive.getAbsolutePath());
            return null;
        }
        if (archiveFiles.length < 1) {
            logger.error("Archive [{}] has no files", archive.getAbsolutePath());
            return null;
        }
        logger.trace("archiveFiles.length = {}", archiveFiles.length);
        String topInnerFolder = archiveFiles[0];
        if (topInnerFolder == null) {
            logger.error("Failed to get top inner folder of archive [{}]", archive.getAbsolutePath());
            return null;
        }
        logger.trace("topInnerFolder: {}", topInnerFolder);
        String folder = new File(archive.getParentFile().getAbsolutePath() + "/" + topInnerFolder).getAbsolutePath();
        if (folder == null || folder.equals("")) {
            logger.error("Improperly formatted archive lacking internal top level directory");
            return null;
        }
        logger.debug("folder: {}", folder);
        TFile folderFS = new TFile(folder);
        if (folderFS.exists()) {
            logger.trace("Archive top level directory already exists, deleting");
            try {
                folderFS.rm_r();
                TVFS.umount();
            } catch (IOException e) {
                logger.error("Failed to remove existing directory [{}] {}:{}", folder, e.getClass().getName(), e.getMessage());
                return null;
            }
        }*/
        try {
            if (archive.getParentFile() == null) {
                logger.error("archive.getParentFile() == null");
                return false;
            }
            logger.trace("Setting bag = [{}]", archive.getParentFile().getAbsolutePath());
            String archiveParent = archive.getParent();
            if (archiveParent == null) {
                logger.error("Archive [{}] has a null parent directory", archive);
                return false;
            }
            logger.trace("archive.getParent() = {}", (archiveParent != null) ? archiveParent : "null");
            TFile bag = new TFile(archive.getParent());
            if (!bag.isDirectory()) {
                logger.error("Bag [{}] is not a directory", bag.getAbsolutePath());
                return false;
            }
            logger.debug("archive: {}", archive.getAbsolutePath());
            logger.debug("bag: {}", bag.getAbsolutePath());
            TFile.cp_rp(archive, bag, TArchiveDetector.NULL, TArchiveDetector.NULL);
            try {
                TVFS.umount();
                logger.trace("[{}] successfully unboxed", archive.getAbsolutePath());
                return true;
            } catch (FsSyncException e) {
                logger.error("unBoxIt : Failed to sync changes to the filesystem: ", e.getMessage());
                return false;
            }
        } catch (IOException ioe) {
            logger.error("Failed to unbox [{}]: {}:{}:{}", archive.getAbsolutePath(), ioe.getClass().getCanonicalName(), ioe.getMessage(), ioe.getCause().toString());
            return false;
        }
    }

    /**
     * Reads a directory to a Bag object
     * @param path Path to the bag directory
     * @return Resulting Bag object
     */
    public static Bag readBag(Path path) {
        logger.trace("Call to readBag({})", path.toAbsolutePath());
        BagReader reader = new BagReader();
        try {
            return reader.read(path);
        } catch (IOException e) {
            logger.error("readBag : Failed to load BagIt bag: {}", path.toAbsolutePath());
            return null;
        } catch (UnparsableVersionException e) {
            logger.error("readBag : Cannot parse this version of BagIt.");
            return null;
        } catch (MaliciousPathException e) {
            logger.error("readBag : Invalid BagIt bag path encountered.");
            return null;
        } catch (UnsupportedAlgorithmException e) {
            logger.error("readBag : BagIt bag requires an unsupported hashing algorithm.");
            return null;
        } catch (InvalidBagitFileFormatException e) {
            logger.error("readBag : The format of this BagIt bag is invalid.");
            return null;
        }
    }

    /**
     * Reads a directory to a Bag object
     * @param path Path to the bag directory
     * @return Resulting Bag object
     */
    public static Bag readBag(File path) {
        return readBag(path.toPath());
    }

    /**
     * Reads a directory to a Bag object
     * @param path Path to the bag directory
     * @return Resulting Bag object
     */
    public static Bag readBag(String path) {
        return readBag(new File(path));
    }

    /**
     * Verifies the bag at the given path
     * @param path Path of the bag to verify
     * @param includeHiddenFiles Whether the bag included hidden files
     * @return Whether the bag is valid or not
     */
    public static boolean verifyBag(Path path, boolean includeHiddenFiles) {
        logger.trace("Call to verifyBag({}, {})", path.toAbsolutePath(), includeHiddenFiles);
        LargeBagVerifier verifier = new LargeBagVerifier();
        Bag bag = readBag(path);
        if (bag == null)
            return false;
        try {
            verifier.isValid(bag, includeHiddenFiles);
            verifier.close();
            return true;
        } catch (IOException e) {
            logger.error("verifyBag : Failed to read a file in BagIt bag : {}", e.getMessage());
            return false;
        } catch (UnsupportedAlgorithmException e) {
            logger.error("verifyBag : BagIt bag requires an unsupported hashing algorithm.");
            return false;
        } catch (MissingPayloadManifestException e) {
            logger.error("verifyBag : BagIt bag is missing a payload manifest.");
            return false;
        } catch (MissingBagitFileException e) {
            logger.error("verifyBag : BagIt bag is missing a file: {}", e.getMessage());
            return false;
        } catch (MissingPayloadDirectoryException e) {
            logger.error("verifyBag : BagIt bag is missing a payload directory.");
            return false;
        } catch (FileNotInPayloadDirectoryException e) {
            logger.error("verifyBag : BagIt bag is missing a file from its payload directory: {}", e.getMessage());
            return false;
        } catch (InterruptedException e) {
            logger.error("verifyBag : Verification process was interrupted.");
            return false;
        } catch (MaliciousPathException e) {
            logger.error("verifyBag : Invalid BagIt bag path encountered.");
            return false;
        } catch (CorruptChecksumException e) {
            logger.error("verifyBag : BagIt bag contains a corrupt checksum: {}", e.getMessage());
            return false;
        } catch (VerificationException e) {
            logger.error("verifyBag : BagIt bag encountered an unknown verification issue.");
            return false;
        } catch (InvalidBagitFileFormatException e) {
            logger.error("verifyBag : BagIt bag is in an invalid format.");
            return false;
        }
    }

    /**
     * Verifies the bag at the given path
     * @param path Path of the bag to verify
     * @param includeHiddenFiles Whether the bag included hidden files
     * @return Whether the bag is valid or not
     */
    public static boolean verifyBag(File path, boolean includeHiddenFiles) {
        return verifyBag(path.toPath(), includeHiddenFiles);
    }

    /**
     * Verifies the bag at the given path
     * @param path Path of the bag to verify
     * @param includeHiddenFiles Whether the bag included hidden files
     * @return Whether the bag is valid or not
     */
    public static boolean verifyBag(String path, boolean includeHiddenFiles) {
        return verifyBag(new File(path), includeHiddenFiles);
    }

    /**
     * Copies the files from one directory to another
     * @param src Source directory to copy files from
     * @param dst Destination directory to copy files to
     * @throws IOException
     */
    private static void copyFolderContents(File src, File dst) throws IOException {
        //logger.trace("Call to copyFolderContents({},{})", src.getAbsolutePath(), dst.getAbsolutePath());
        if (src.isDirectory()) {
            if (!dst.exists())
                dst.mkdir();
            String files[] = src.list();
            for (String file : files) {
                File srcFile = new File(src, file);
                File destFile = new File(dst, file);
                copyFolderContents(srcFile,destFile);
            }
        } else
            Files.move(Paths.get(src.toURI()), Paths.get(dst.toURI()));
    }

    /**
     * Deletes an entire folder structure
     * @param folder Path of the folder to delete
     * @throws IOException Thrown from sub-routines
     */
    private static void deleteFolder(Path folder) throws IOException {
        logger.trace("Call to deleteFolder({})", folder.toAbsolutePath());
        Files.walkFileTree(folder, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
