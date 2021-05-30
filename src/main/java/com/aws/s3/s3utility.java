package com.aws.s3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.*;
import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;


public class s3utility {
    private static AmazonS3 s3Client;
    private static TransferManager xfer_mgr;
    private static String awsAccessKey1,awsSecretKey1,sessionToken1,region1;

    public static void setAwsCredentials(String awsAccessKey,String awsSecretKey,String sessionToken,String region) {
        awsAccessKey1 = awsAccessKey;
        awsSecretKey1 = awsSecretKey;
        sessionToken1 = sessionToken;
        region1 = region;
    }

    public static void resumeUpload(PersistableUpload persistableUpload, PutObjectRequest object  ,CountDownLatch doneSignal,String ResumeCommandFilePath) throws Exception {
        try {
            BasicSessionCredentials sessionCredentials = new BasicSessionCredentials(
                    awsAccessKey1, awsSecretKey1,
                    sessionToken1);
            String region = region1;

            s3Client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(sessionCredentials))
                    .withRegion(region)
                    .build();
            TransferManagerBuilder transferManagerBuilder = TransferManagerBuilder.standard().withMinimumUploadPartSize((long) 5 * 1024 * 1024).withS3Client(s3Client);
            transferManagerBuilder.setShutDownThreadPools(false);
            TransferManager xfer_mgr = transferManagerBuilder.build();

            progressListener listner = new progressListener(object.getFile(), object.getBucketName() + "/" + object.getKey(), doneSignal, ResumeCommandFilePath);
            Upload u = xfer_mgr.resumeUpload(persistableUpload);
            u.addProgressListener(listner);
            listner.setUploadPointer_transferManager(u, xfer_mgr);
        } catch (Exception e) {
            e.printStackTrace();

        }
    }

    public static void resumeDownload(PersistableDownload persistableDownload, PutObjectRequest object  ,CountDownLatch doneSignal,String ResumeCommandFilePath) throws Exception {
        try {
            BasicSessionCredentials sessionCredentials = new BasicSessionCredentials(
                    awsAccessKey1, awsSecretKey1,
                    sessionToken1);
            String region = region1;

            s3Client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(sessionCredentials))
                    .withRegion(region)
                    .build();
            TransferManagerBuilder transferManagerBuilder = TransferManagerBuilder.standard().withS3Client(s3Client);
            transferManagerBuilder.setShutDownThreadPools(false);
            TransferManager xfer_mgr = transferManagerBuilder.build();

            progressListener listner = new progressListener(object.getFile(), object.getBucketName() + "/" + object.getKey(), doneSignal, ResumeCommandFilePath);
            Download d = xfer_mgr.resumeDownload(persistableDownload);
            d.addProgressListener(listner);
            listner.setDownloadPointer_transferManager(d, xfer_mgr);
        } catch (Exception e) {
            e.printStackTrace();

        }
    }

    public static Upload uploadFilesWithListener(PutObjectRequest object,CountDownLatch doneSignal,String UploadCommandFilePath) throws Exception {
        try {
            BasicSessionCredentials sessionCredentials = new BasicSessionCredentials(
                    awsAccessKey1, awsSecretKey1,
                    sessionToken1);
            String region = region1;

            s3Client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(sessionCredentials))
                    .withRegion(region)
                    .build();
            TransferManagerBuilder transferManagerBuilder = TransferManagerBuilder.standard()
                    .withExecutorFactory(() -> Executors.newFixedThreadPool(50))
                    .withMinimumUploadPartSize((long) 5 * 1024 * 1024)
                    .withS3Client(s3Client);
            transferManagerBuilder.setShutDownThreadPools(false);
            TransferManager xfer_mgr = transferManagerBuilder.build();

            progressListener listner;

            listner = new progressListener(object.getFile(), object.getBucketName() + "/" + object.getKey(), doneSignal, UploadCommandFilePath);
            Upload u = xfer_mgr.upload(object);
            listner.setUploadPointer_transferManager(u, xfer_mgr);
            u.addProgressListener(listner);

            // block with Transfer.waitForCompletion()
            //u.waitForCompletion();
            /*
            try {
                doneSignal.await();
            } catch (InterruptedException e) {
                throw new Exception("Couldn't wait for all uploads to be finished");
            }
            */
            return u;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    public static Download downloadFilesWithListener(GetObjectRequest object, File file,CountDownLatch doneSignal,String DownloadCommandFilePath) throws Exception {
        try {
            BasicSessionCredentials sessionCredentials = new BasicSessionCredentials(
                    awsAccessKey1, awsSecretKey1,
                    sessionToken1);
            String region = region1;

            s3Client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(sessionCredentials))
                    .withRegion(region)
                    .build();
            TransferManagerBuilder transferManagerBuilder = TransferManagerBuilder.standard()
                    .withExecutorFactory(() -> Executors.newFixedThreadPool(50))
                    .withS3Client(s3Client);
            transferManagerBuilder.setShutDownThreadPools(false);
            TransferManager xfer_mgr = transferManagerBuilder.build();

            progressListener listner;
            listner = new progressListener(file, object.getBucketName() + "/" + object.getKey(), doneSignal, DownloadCommandFilePath);
            Download d = xfer_mgr.download(object, file);
            listner.setDownloadPointer_transferManager(d, xfer_mgr);
            d.addProgressListener(listner);

            return d;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

}
