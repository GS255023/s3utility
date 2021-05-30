package com.aws.s3;

import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardWatchEventKinds.*;

/**
 * Example to watch a directory (or tree) for changes to files.
 */

public class WatchDir {

    private final WatchService watcher;
    private final Map<WatchKey, Path> keys;
    private final boolean recursive;
    private boolean trace = false;
    private static Upload uploadPointer;
    private static Download downloadPointer;
    private HashMap<String, Upload> uploads = new HashMap<String, Upload>();
    private HashMap<String, Download> downloads = new HashMap<String, Download>();
    private List<PutObjectRequest> putobjectList = new ArrayList<PutObjectRequest>();
    private List<GetObjectRequest> getobjectList = new ArrayList<GetObjectRequest>();
    private CountDownLatch doneSignal;
    private static Path dir;

    @SuppressWarnings("unchecked")
    static <T> WatchEvent<T> cast(WatchEvent<?> event) {
        return (WatchEvent<T>) event;
    }

    /**
     * Register the given directory with the WatchService
     */
    private void register(Path dir) throws IOException {
        WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
        if (trace) {
            Path prev = keys.get(key);
            if (prev == null) {
                System.out.format("register: %s\n", dir);
            } else {
                if (!dir.equals(prev)) {
                    System.out.format("update: %s -> %s\n", prev, dir);
                }
            }
        }
        keys.put(key, dir);
    }

    /**
     * Register the given directory, and all its sub-directories, with the
     * WatchService.
     */
    private void registerAll(final Path start) throws IOException {
        // register directory and sub-directories
        Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                    throws IOException {
                register(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * Creates a WatchService and registers the given directory
     */
    WatchDir(Path dir, boolean recursive) throws IOException {
        this.watcher = FileSystems.getDefault().newWatchService();
        this.keys = new HashMap<WatchKey, Path>();
        this.recursive = recursive;

        if (recursive) {
            System.out.format("Scanning %s ...\n", dir);
            registerAll(dir);
            System.out.println("Done.");
        } else {
            register(dir);
        }

        // enable trace after initial registration
        this.trace = true;
    }

    Long getepochTime() {
        Date today = Calendar.getInstance().getTime();

        // Constructs a SimpleDateFormat using the given pattern
        SimpleDateFormat crunchifyFormat = new SimpleDateFormat("MMM dd yyyy HH:mm:ss.SSS zzz");

        // format() formats a Date into a date/time string.
        String currentTime = crunchifyFormat.format(today);

        try {

            // parse() parses text from the beginning of the given string to produce a date.
            Date date = crunchifyFormat.parse(currentTime);

            // getTime() returns the number of milliseconds since January 1, 1970, 00:00:00 GMT represented by this Date object.
            long epochTime = date.getTime();
            return epochTime;

        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Process all events for keys queued to the watcher
     */
    void processEvents() throws Exception {
        for (; ; ) {

            // wait for key to be signalled
            WatchKey key;
            try {
                key = watcher.take();
            } catch (InterruptedException x) {
                return;
            }

            Path dir = keys.get(key);
            if (dir == null) {
                System.err.println("WatchKey not recognized!!");
                continue;
            }

            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind kind = event.kind();

                // TBD - provide example of how OVERFLOW event is handled
                if (kind == OVERFLOW) {
                    continue;
                }

                // Context for directory entry event is the file name of entry
                WatchEvent<Path> ev = cast(event);
                Path name = ev.context();
                Path child = dir.resolve(name);
                String filePath = child.toString().replace('\\', '/');
                String fileName = child.getFileName().toString().toLowerCase();

                // print out event
                if (event.kind().name() == "ENTRY_CREATE" && fileName.contains("upload")) {
                    System.out.format("\n%s: %s\n", event.kind().name(), filePath);
                    Thread.sleep(5000);
                    List<List<String>> records = new ArrayList<>();
                    try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                        String line;
                        String PIPE_DELIMITER = ",";
                        int header = 0;
                        while ((line = br.readLine()) != null) {
                            String[] values = line.split(PIPE_DELIMITER, 5);
                            if (header > 0)
                                records.add(Arrays.asList(values));
                            header++;
                        }
                    }

                    doneSignal = new CountDownLatch(records.size());
                    for (List l1 : records) {
                        String bucketName = l1.get(0).toString();
                        String keyfile = l1.get(1).toString();
                        String acl = l1.get(2).toString();
                        String localFilePath = l1.get(3).toString();
                        PutObjectRequest object = new PutObjectRequest(bucketName, keyfile,
                                new File(localFilePath)).withCannedAcl(CannedAccessControlList.valueOf(acl));
                        putobjectList.add(object);
                        uploadPointer = s3utility.uploadFilesWithListener(object, doneSignal, filePath);
                        uploads.put(bucketName + "/" + keyfile, uploadPointer);
                    }

                } else if (event.kind().name() == "ENTRY_CREATE" && fileName.contains("pauseupload")) {
                    System.out.format("\n%s: %s\n", event.kind().name(), filePath);
                    // System.out.format("%s: \n", doneSignal.getCount());
                    Thread.sleep(5000);
                    List<List<String>> records = new ArrayList<>();

                    try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                        String line;
                        String PIPE_DELIMITER = ",";
                        int header = 0;
                        while ((line = br.readLine()) != null) {
                            String[] values = line.split(PIPE_DELIMITER, 5);
                            if (header > 0)
                                records.add(Arrays.asList(values));
                            header++;
                        }
                    }

                    for (List l1 : records) {
                        String bucketName = l1.get(0).toString();
                        String keyfile = l1.get(1).toString();
                        String acl = l1.get(2).toString();
                        String localFilePath = l1.get(3).toString();


                        //System.out.format("%s: %s\n", event.kind().name(), child.toString().replace('\\', '/'));
                        boolean forceCancel = true;
                        Upload g = uploads.get(bucketName + "/" + keyfile);
                        PauseResult<PersistableUpload> pauseResult = g.tryPause(forceCancel);

                        // Retrieve the persistable upload from the pause result.
                        PersistableUpload persistableUpload = pauseResult.getInfoToResume();

                        // Create a new file to store the information.
                        File f = new File(dir.toString().replace('\\', '/') + "/resume/" + getepochTime().toString() + "-resumeupload");
                        if (!f.exists()) f.createNewFile();
                        FileOutputStream fos = new FileOutputStream(f);

                        // Serialize the persistable upload to the file.
                        persistableUpload.serialize(fos);
                        fos.close();
                    }
                } else if (event.kind().name() == "ENTRY_CREATE" && fileName.contains("resumeupload")) {

                    System.out.format("\n%s: %s\n", event.kind().name(), filePath);
                    Thread.sleep(5000);

                    try (FileInputStream fis = new FileInputStream(new File(filePath))) {

                        // Deserialize PersistableUpload information from disk.
                        PersistableUpload persistableUpload = PersistableTransfer.deserializeFrom(fis);

                        JSONParser parser = new JSONParser();
                        try {
                            Object obj = parser.parse(new FileReader(filePath));

                            // A JSON object. Key value pairs are unordered. JSONObject supports java.util.Map interface.
                            JSONObject jsonObject = (JSONObject) obj;
                            String bucketName = jsonObject.get("bucketName").toString();
                            String keyfile = jsonObject.get("key").toString();
                            String localFilePath = jsonObject.get("file").toString();

                            PutObjectRequest object = new PutObjectRequest(bucketName, keyfile,
                                    new File(localFilePath)).withCannedAcl(CannedAccessControlList.Private);

                            doneSignal = new CountDownLatch(1);
                            s3utility.resumeUpload(persistableUpload, object, doneSignal, filePath);

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        fis.close();
                    }
                }
                // print out event
                else if (event.kind().name() == "ENTRY_CREATE" && fileName.contains("download")) {
                    System.out.format("\n%s: %s\n", event.kind().name(), filePath);
                    Thread.sleep(5000);

                    List<List<String>> records = new ArrayList<>();
                    try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                        String line;
                        String PIPE_DELIMITER = ",";
                        int header = 0;
                        while ((line = br.readLine()) != null) {
                            String[] values = line.split(PIPE_DELIMITER, 5);
                            if (header > 0)
                                records.add(Arrays.asList(values));
                            header++;
                        }
                    }

                    doneSignal = new CountDownLatch(records.size());

                    for (List l1 : records) {
                        String bucketName = l1.get(0).toString();
                        String keyfile = l1.get(1).toString();
                        String localFilePath = l1.get(2).toString();
                        GetObjectRequest object = new GetObjectRequest(bucketName, keyfile);
                        getobjectList.add(object);
                        downloadPointer = s3utility.downloadFilesWithListener(object, new File(localFilePath), doneSignal, filePath);
                        downloads.put(bucketName + "/" + keyfile, downloadPointer);
                    }
                }
                else if (event.kind().name() == "ENTRY_CREATE" && fileName.contains("pausedownload")) {
                    System.out.format("\n%s: %s\n", event.kind().name(), filePath);
                    // System.out.format("%s: \n", doneSignal.getCount());
                    Thread.sleep(5000);
                    List<List<String>> records = new ArrayList<>();

                    try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                        String line;
                        String PIPE_DELIMITER = ",";
                        int header = 0;
                        while ((line = br.readLine()) != null) {
                            String[] values = line.split(PIPE_DELIMITER, 5);
                            if (header > 0)
                                records.add(Arrays.asList(values));
                            header++;
                        }
                    }

                    for (List l1 : records) {
                        String bucketName = l1.get(0).toString();
                        String keyfile = l1.get(1).toString();
                        String acl = l1.get(2).toString();
                        String localFilePath = l1.get(3).toString();

                        //System.out.format("%s: %s\n", event.kind().name(), child.toString().replace('\\', '/'));
                        Download g = downloads.get(bucketName + "/" + keyfile);
                        PersistableDownload persistableDownload = g.pause();

                        // Create a new file to store the information.
                        File f = new File(dir.toString().replace('\\', '/') + "/resume/" + getepochTime().toString() + "-resumedownload");
                        if (!f.exists()) f.createNewFile();
                        FileOutputStream fos = new FileOutputStream(f);

                        // Serialize the persistable download to a file.
                        persistableDownload.serialize(fos);
                        fos.close();
                    }
                }else if (event.kind().name() == "ENTRY_CREATE" && fileName.contains("resumedownload")) {

                    System.out.format("\n%s: %s\n", event.kind().name(), filePath);
                    Thread.sleep(5000);

                    try (FileInputStream fis = new FileInputStream(new File(filePath))) {

                        // Deserialize PersistableUpload information from disk.
                        PersistableDownload persistDownload = PersistableTransfer.deserializeFrom(fis);

                        JSONParser parser = new JSONParser();
                        try {
                            Object obj = parser.parse(new FileReader(filePath));

                            // A JSON object. Key value pairs are unordered. JSONObject supports java.util.Map interface.
                            JSONObject jsonObject = (JSONObject) obj;
                            String bucketName = jsonObject.get("bucketName").toString();
                            String keyfile = jsonObject.get("key").toString();
                            String localFilePath = jsonObject.get("file").toString();

                            PutObjectRequest object = new PutObjectRequest(bucketName, keyfile,
                                    new File(localFilePath)).withCannedAcl(CannedAccessControlList.Private);

                            doneSignal = new CountDownLatch(1);
                            s3utility.resumeDownload(persistDownload, object, doneSignal, filePath);

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        fis.close();
                    }
                }

                // if directory is created, and watching recursively, then
                // register it and its sub-directories
                if (recursive && (kind == ENTRY_CREATE)) {
                    try {
                        if (Files.isDirectory(child, NOFOLLOW_LINKS)) {
                            registerAll(child);
                        }
                    } catch (IOException x) {
                        // ignore to keep sample readbale
                    }
                }
            }

            // reset key and remove from set if directory no longer accessible
            boolean valid = key.reset();
            if (!valid) {
                keys.remove(key);

                // all directories are inaccessible
                if (keys.isEmpty()) {
                    break;
                }
            }
        }
    }

    static void usage() {
        System.err.println("usage: java WatchDir [-r] dir");
        System.exit(-1);
    }

    public static void main(String[] args) throws Exception {
        // parse arguments
        if (args.length == 0 || args.length > 2)
            usage();
        boolean recursive = false;
        int dirArg = 0;
        if (args[0].equals("-r")) {
            if (args.length < 2)
                usage();
            recursive = true;
            dirArg++;
        }

        // register directory and process its events
        dir = Paths.get(args[dirArg]);

        String filePath = dir.toString().replace('\\', '/') + "/config/s3credentials";

        if (Files.exists(Paths.get(filePath))) {
            JSONParser parser = new JSONParser();
            try {

                File directory1 = new File(dir.toString().replace('\\', '/') + "/processed");
                if (!directory1.exists()) {
                    directory1.mkdirs();
                    // If you require it to make the entire directory path including parents,
                    // use directory.mkdirs(); here instead.
                }
                File directory2 = new File(dir.toString().replace('\\', '/') + "/resume");
                if (!directory2.exists()) {
                    directory2.mkdirs();
                    // If you require it to make the entire directory path including parents,
                    // use directory.mkdirs(); here instead.
                }

                Object obj = parser.parse(new FileReader(filePath));

                // A JSON object. Key value pairs are unordered. JSONObject supports java.util.Map interface.
                JSONObject jsonObject = (JSONObject) obj;
                String awsAccessKey = jsonObject.get("AWS_ACCESS_KEY_ID").toString();
                String awsSecretKey = jsonObject.get("AWS_SECRET_ACCESS_KEY").toString();
                String sessionToken = jsonObject.get("AWS_SESSION_TOKEN").toString();
                String region = jsonObject.get("region").toString();
                s3utility.setAwsCredentials(awsAccessKey, awsSecretKey, sessionToken, region);
                new WatchDir(dir, recursive).processEvents();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            System.err.println("AWS configuration file does not exist: " + filePath);
            System.exit(-1);
        }

    }
}