package com.researchworx.cresco.plugins.gobjectIngestion.objectstorage;

import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.plugin.core.CPlugin;
import com.researchworx.cresco.library.utilities.CLogger;
import gov.loc.repository.bagit.domain.Bag;
import gov.loc.repository.bagit.domain.Manifest;
import gov.loc.repository.bagit.exceptions.*;
import gov.loc.repository.bagit.hash.BagitAlgorithmNameToSupportedAlgorithmMapping;
import gov.loc.repository.bagit.hash.StandardBagitAlgorithmNameToSupportedAlgorithmMapping;
import gov.loc.repository.bagit.verify.CheckManifestHashesTask;
import gov.loc.repository.bagit.verify.MandatoryVerifier;
import gov.loc.repository.bagit.verify.PayloadVerifier;
import gov.loc.repository.bagit.verify.QuickVerifier;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

public class LargeBagVerifier implements AutoCloseable {
    private static CLogger logger = new CLogger(Encapsulation.class, new LinkedBlockingQueue<>(), "", "", "");

    public static void setLogger(CPlugin plugin) {
        logger = new CLogger(ObjectEngine.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Trace);
    }
    private static final ResourceBundle messages = ResourceBundle.getBundle("MessageBundle");
    private static final int max_batch_size = 100;

    private final PayloadVerifier manifestVerifier;
    private final ExecutorService executor;

    /**
     * Create a BagVerifier with a cached thread pool and a
     * {@link StandardBagitAlgorithmNameToSupportedAlgorithmMapping}
     */
    public LargeBagVerifier(){
        this(Executors.newCachedThreadPool(), new StandardBagitAlgorithmNameToSupportedAlgorithmMapping());
    }

    /**
     * Create a BagVerifier with a cached thread pool and a custom mapping
     *
     * @param nameMapping the mapping between BagIt algorithm name and the java supported algorithm
     */
    public LargeBagVerifier(final BagitAlgorithmNameToSupportedAlgorithmMapping nameMapping){
        this(Executors.newCachedThreadPool(), nameMapping);
    }

    /**
     * Create a BagVerifier with a custom thread pool and a
     * {@link StandardBagitAlgorithmNameToSupportedAlgorithmMapping}
     *
     * @param executor the thread pool to use when doing work
     */
    public LargeBagVerifier(final ExecutorService executor){
        this(executor, new StandardBagitAlgorithmNameToSupportedAlgorithmMapping());
    }

    /**
     * Create a BagVerifier with a custom thread pool and a custom mapping
     *
     * @param nameMapping the mapping between BagIt algorithm name and the java supported algorithm
     * @param executor the thread pool to use when doing work
     */
    public LargeBagVerifier(final ExecutorService executor, final BagitAlgorithmNameToSupportedAlgorithmMapping nameMapping){
        manifestVerifier = new PayloadVerifier(nameMapping, executor);
        this.executor = executor;
    }

    @Override
    public void close() throws SecurityException{
        //shutdown the thread pool so the resource isn't leaked
        executor.shutdown();
        manifestVerifier.close();
    }

    /**
     * Determine if we can quickly verify by comparing the number of files and the total number of bytes expected
     *
     * @param bag the {@link Bag} object you wish to check
     * @return true if the bag can be quickly verified
     */
    public static boolean canQuickVerify(final Bag bag){
        return QuickVerifier.canQuickVerify(bag);
    }

    /**
     * Quickly verify by comparing the number of files and the total number of bytes expected
     *
     * @param bag the bag to verify by payload-oxum
     *
     * @throws IOException if there is an error reading a file
     * @throws InvalidPayloadOxumException if either the total bytes or the number of files
     * calculated for the payload directory of the bag is different than the supplied values
     * @throws PayloadOxumDoesNotExistException if the bag does not contain a payload-oxum.
     * To check, run {@link BagVerifier#canQuickVerify}
     */
    public static void quicklyVerify(final Bag bag) throws IOException, InvalidPayloadOxumException{
        QuickVerifier.quicklyVerify(bag);
    }

    /**
     * See <a href="https://tools.ietf.org/html/draft-kunze-bagit-13#section-3">https://tools.ietf.org/html/draft-kunze-bagit-13#section-3</a><br>
     *  A bag is <b>valid</b> if the bag is complete and every checksum has been
     *  verified against the contents of its corresponding file.
     *
     * @param bag the {@link Bag} object to check
     * @param ignoreHiddenFiles ignore hidden files unless explicitly listed in manifest(s)
     *
     * @throws CorruptChecksumException when the computed hash doesn't match given hash
     * @throws IOException if there was an error with the file
     * @throws MissingPayloadManifestException if there is not at least one payload manifest
     * @throws MissingBagitFileException  if there is no bagit.txt file
     * @throws MissingPayloadDirectoryException if there is no /data directory
     * @throws FileNotInPayloadDirectoryException if a manifest lists a file but it is not in the payload directory
     * @throws InterruptedException if the threads are interrupted when checking if all files are listed in manifest(s)
     * @throws MaliciousPathException if there is path that is referenced in the manifest that is outside the bag root directory
     * @throws VerificationException some other exception happened during processing so capture it here.
     * @throws UnsupportedAlgorithmException if the manifest uses a algorithm that isn't supported
     * @throws InvalidBagitFileFormatException if the manifest is not formatted properly
     */
    public void isValid(final Bag bag, final boolean ignoreHiddenFiles) throws IOException, MissingPayloadManifestException, MissingBagitFileException, MissingPayloadDirectoryException, FileNotInPayloadDirectoryException, InterruptedException, MaliciousPathException, CorruptChecksumException, VerificationException, UnsupportedAlgorithmException, InvalidBagitFileFormatException{
        logger.info(messages.getString("checking_bag_is_valid"), bag.getRootDir());
        isComplete(bag, ignoreHiddenFiles);

        logger.debug(messages.getString("checking_payload_checksums"));
        for(final Manifest payloadManifest : bag.getPayLoadManifests()){
            checkHashes(payloadManifest);
        }

        logger.debug(messages.getString("checking_tag_file_checksums"));
        for(final Manifest tagManifest : bag.getTagManifests()){
            checkHashes(tagManifest);
        }
    }

    /*
     * Check the supplied checksum hashes against the generated checksum hashes
     */
    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    void checkHashes(final Manifest manifest) throws CorruptChecksumException, InterruptedException, VerificationException{
        LinkedList<Map.Entry<Path, String>> toProcess = new LinkedList<>();
        toProcess.addAll(manifest.getFileToChecksumMap().entrySet());

        while (toProcess.size() > 0) {
            HashMap<Path, String> batchToProcess = new HashMap<>();
            while (batchToProcess.size() < max_batch_size && toProcess.size() > 0) {
                Map.Entry<Path, String> entry = toProcess.pop();
                batchToProcess.put(entry.getKey(), entry.getValue());
            }
            final CountDownLatch latch = new CountDownLatch(batchToProcess.size());

            //TODO maybe return all of these at some point...
            final Collection<Exception> exceptions = Collections.synchronizedCollection(new ArrayList<Exception>());

            for (final Map.Entry<Path, String> entry : batchToProcess.entrySet()) {
                executor.execute(new CheckManifestHashesTask(entry, manifest.getAlgorithm().getMessageDigestName(), latch, exceptions));
            }

            latch.await();

            if (!exceptions.isEmpty()) {
                final Exception e = exceptions.iterator().next();
                if (e instanceof CorruptChecksumException) {
                    logger.debug(messages.getString("checksums_not_matching_error"), exceptions.size());
                    throw (CorruptChecksumException) e;
                }

                throw new VerificationException(e);
            }
        }
    }

    /**
     * See <a href="https://tools.ietf.org/html/draft-kunze-bagit-13#section-3">https://tools.ietf.org/html/draft-kunze-bagit-13#section-3</a><br>
     * A bag is <b>complete</b> if <br>
     * <ul>
     * <li>every element is present
     * <li>every file in the payload manifest(s) are present
     * <li>every file in the tag manifest(s) are present. Tag files not listed in a tag manifest may be present.
     * <li>every file in the data directory must be listed in at least one payload manifest
     * <li>each element must comply with the bagit spec
     * </ul>
     *
     * @param bag the {@link Bag} object to check
     * @param ignoreHiddenFiles ignore hidden files unless explicitly listed in manifest(s)
     *
     * @throws IOException if there was an error with the file
     * @throws MissingPayloadManifestException if there is not at least one payload manifest
     * @throws MissingBagitFileException  if there is no bagit.txt file
     * @throws MissingPayloadDirectoryException if there is no /data directory
     * @throws FileNotInPayloadDirectoryException if a manifest lists a file but it is not in the payload directory
     * @throws InterruptedException if the threads are interrupted when checking if all files are listed in manifest(s)
     * @throws MaliciousPathException if there is path that is referenced in the manifest that is outside the bag root directory
     * @throws UnsupportedAlgorithmException if the manifest uses a algorithm that isn't supported
     * @throws InvalidBagitFileFormatException if the manifest is not formatted properly
     */
    public void isComplete(final Bag bag, final boolean ignoreHiddenFiles) throws
            IOException, MissingPayloadManifestException, MissingBagitFileException, MissingPayloadDirectoryException,
            FileNotInPayloadDirectoryException, InterruptedException, MaliciousPathException, UnsupportedAlgorithmException, InvalidBagitFileFormatException{
        logger.info(messages.getString("checking_bag_is_complete"), bag.getRootDir());

        MandatoryVerifier.checkFetchItemsExist(bag.getItemsToFetch(), bag.getRootDir());

        MandatoryVerifier.checkBagitFileExists(bag.getRootDir(), bag.getVersion());

        MandatoryVerifier.checkPayloadDirectoryExists(bag);

        MandatoryVerifier.checkIfAtLeastOnePayloadManifestsExist(bag.getRootDir(), bag.getVersion());

        manifestVerifier.verifyPayload(bag, ignoreHiddenFiles);
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    public PayloadVerifier getManifestVerifier() {
        return manifestVerifier;
    }
}
