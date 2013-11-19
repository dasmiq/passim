/*
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

package jaligner.ui.util;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.print.DocPrintJob;
import javax.print.event.PrintJobAdapter;
import javax.print.event.PrintJobEvent;

/**
 * Mointor for print job.
 * 
 * @author Ahmed Moustafa
 */

public class PrintJobMointor {
	/**
	 * Logger
	 */
	private static final Logger logger = Logger.getLogger(TextComponentUtil.class.getName());

    // True iff it is safe to close the print job's input stream
    private boolean done = false;
    
    PrintJobMointor(DocPrintJob job) {
        // Add a listener to the print job
        job.addPrintJobListener(new PrintJobAdapter() {
            public void printJobCanceled(PrintJobEvent printJobEvent) {
                logger.info("Print job canceled");
            	allDone();
            }
            
            public void printJobCompleted(PrintJobEvent printJobEvent) {
            	logger.info("Print job completed");
                allDone();
            }
            
            public void printJobFailed(PrintJobEvent printJobEvent) {
            	logger.info("Print job failed");
                allDone();
            }

            public void printJobNoMoreEvents(PrintJobEvent printJobEvent) {
                allDone();
            }

            void allDone() {
                synchronized (PrintJobMointor.this) {
                    done = true;
                    PrintJobMointor.this.notify();
                }
            }
        });
    }

    /**
     * Waits for print job
     *
     */
    public synchronized void waitForPrintJob() {
        try {
        	logger.info("Waiting for print job...");
            while (!done) {
                wait();
            }
            logger.info("Finished waiting for print");
        } catch (InterruptedException e) {
        	logger.log(Level.SEVERE, "Failed waiting for print job: " + e.getMessage(), e);
        }
    }
}
