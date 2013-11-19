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
 
package jaligner.ui.clipboard;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Checks the system clipboard to notifies a listener with current contents.
 * 
 * @author Ahmed Moustafa
 */

public class ClipboardPoller extends Thread {
	/**
	 * Sleeping interval
	 */
	private static final int SLEEPING_TIME_IN_MILLISECONDS = 1500;
	
	/**
	 * Clipboard listener {@link ClipboardListener}
	 */
	private ClipboardListener listener = null;
	
	/**
	 * Running flag
	 */
	private boolean running = false;
	
	/**
	 * Logger
	 */
	private Logger logger = null;
	
	/**
	 * Constructor
	 * @param listener
	 */
	public ClipboardPoller(ClipboardListener listener) {
		logger = Logger.getLogger(this.getClass().getName());
	    this.listener = listener;
	}
	
	/**
	 * Runs the thread
	 */
	public void run() {
	    logger.info("Started");
		while (running) {
			String contents = ClipboardHandlerFactory.getClipboardHandler().getContents();
			listener.clipboardCheck(contents);
			try {
				Thread.sleep(SLEEPING_TIME_IN_MILLISECONDS);
			} catch (Exception e){
			    logger.log(Level.SEVERE, "Failed sleeping: " + e.getMessage(), e);
				running = false;
			}
		}
		logger.info("Stopped");
	}
	
	/**
	 * Starts the thread
	 */
	public void start() {
	    logger.info("Starting...");
	    if (!running) {
	        if (listener != null) {
	            running = true;
	        } else {
	            logger.warning("No listener");
	        }
	    } else {
	        logger.warning("Already started");
	    }
	    super.start();
	}
}