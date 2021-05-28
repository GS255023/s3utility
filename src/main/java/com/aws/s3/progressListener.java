package com.aws.s3;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.transfer.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.CountDownLatch;


class progressListener implements ProgressListener {
    private static Log log = LogFactory.getLog(progressListener.class);
    CountDownLatch doneSignal;
    File f;
    String target;
    Upload uploadPointer = null;
    Download downloadPointer = null;
    TransferManager transfer = null;
    String commandFilepath = null;

    public progressListener(File f, String target, CountDownLatch doneSignal, String commandFilepath) {
        this.f = f;
        this.target = target;
        this.doneSignal = doneSignal;
        this.commandFilepath = commandFilepath;
    }

    public void setUploadPointer_transferManager(Upload uploadPointer, TransferManager transfer) {
        this.uploadPointer = uploadPointer;
        this.transfer = transfer;
    }

    public void setDownloadPointer_transferManager(Download downloadPointer, TransferManager transfer) {
        this.downloadPointer = downloadPointer;
        this.transfer = transfer;
    }

    public void progressChanged(ProgressEvent progressEvent) {

        double pct;
        Transfer.TransferState xfer_state;
        if (uploadPointer != null) {
            Upload u = uploadPointer;
            pct = u.getProgress().getPercentTransferred();
            xfer_state = u.getState();
        } else {
            Download d = downloadPointer;
            //(Upload)uploadPointer.get(this.target);
            pct = d.getProgress().getPercentTransferred();
            xfer_state = d.getState();
        }
        eraseProgressBar();
        printProgressBar(pct);

        if (progressEvent.getEventType()
                == ProgressEventType.TRANSFER_STARTED_EVENT) {
            //printProgressBar(0.0);
            System.out.println(": " + xfer_state);
            if (uploadPointer != null) {
                System.out.println("Started to upload: " + f.getAbsolutePath()
                        + " -> " + this.target);
            } else {
                System.out.println("Started to download: " + this.target
                        + " -> " + f.getAbsolutePath());
            }
        }
        if (progressEvent.getEventType()
                == ProgressEventType.TRANSFER_COMPLETED_EVENT) {
            if (uploadPointer != null) {
                System.out.println("Completed upload: " + f.getAbsolutePath()
                        + " -> " + this.target);
            } else {
                System.out.println("Completed download: " + this.target
                        + " -> " + f.getAbsolutePath());
            }
            doneSignal.countDown();
            System.out.format("Countdown %s: \n", doneSignal.getCount());
            if (doneSignal.getCount() <= 0) {
                try {
                    if (Files.exists(Paths.get(commandFilepath))) {
                        Files.copy(Paths.get(commandFilepath), Paths.get(Paths.get(commandFilepath).getParent().toString() + "/processed/success_" + Paths.get(commandFilepath).getFileName().toString()), StandardCopyOption.REPLACE_EXISTING);
                        Files.delete(Paths.get(commandFilepath));
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                transfer.shutdownNow();
            }
        }
        if (progressEvent.getEventType() ==
                ProgressEventType.TRANSFER_FAILED_EVENT) {
            if (uploadPointer != null) {
                System.out.println("Failed upload: " + f.getAbsolutePath()
                        + " -> " + this.target);
            } else {
                System.out.println("Failed download: " + this.target
                        + " -> " + f.getAbsolutePath());
            }
            try {
                if (Files.exists(Paths.get(commandFilepath))) {
                    Files.copy(Paths.get(commandFilepath), Paths.get(Paths.get(commandFilepath).getParent().toString() + "/processed/failed_" + Paths.get(commandFilepath).getFileName().toString()), StandardCopyOption.REPLACE_EXISTING);
                    Files.delete(Paths.get(commandFilepath));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            doneSignal.countDown();
            transfer.shutdownNow();
        }
    }


    // Prints progress while waiting for the transfer to finish.
    public static void showTransferProgress(Transfer xfer) {
        // snippet-start:[s3.java1.s3_xfer_mgr_progress.poll]
        // print the transfer's human-readable description
        System.out.println(xfer.getDescription());
        // print an empty progress bar...
        printProgressBar(0.0);
        // update the progress bar while the xfer is ongoing.
        do {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                return;
            }
            // Note: so_far and total aren't used, they're just for
            // documentation purposes.
            TransferProgress progress = xfer.getProgress();
            long so_far = progress.getBytesTransferred();
            long total = progress.getTotalBytesToTransfer();
            double pct = progress.getPercentTransferred();
            eraseProgressBar();
            printProgressBar(pct);
        } while (xfer.isDone() == false);
        // print the final state of the transfer.
        Transfer.TransferState xfer_state = xfer.getState();
        System.out.println(": " + xfer_state);
        // snippet-end:[s3.java1.s3_xfer_mgr_progress.poll]
    }


    // prints a simple text progressbar: [#####     ]
    public static void printProgressBar(double pct) {
        // if bar_size changes, then change erase_bar (in eraseProgressBar) to
        // match.
        final int bar_size = 40;
        final String empty_bar = "                                        ";
        final String filled_bar = "########################################";
        int amt_full = (int) (bar_size * (pct / 100.0));
        System.out.format("  [%s%s]", filled_bar.substring(0, amt_full),
                empty_bar.substring(0, bar_size - amt_full));
    }

    // erases the progress bar.
    public static void eraseProgressBar() {
        // erase_bar is bar_size (from printProgressBar) + 4 chars.
        final String erase_bar = "\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b";
        System.out.format(erase_bar);
    }

}
