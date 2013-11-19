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

import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.StringSelection;
import java.awt.datatransfer.Transferable;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jnlp.ClipboardService;
import javax.jnlp.ServiceManager;

/**
 * Sets and gets the contents of the system clipboard.  
 * 
 * @author Ahmed Moustafa
 */

public class ClipboardHandlerJNLP implements ClipboardHandler {
	private static Logger logger = Logger.getLogger(ClipboardHandlerJNLP.class.getName());
	
	/**
	 * Gets the contents of the system clipboard
	 * 
	 * @return The text system clipboad contents 
	 */
	public String getContents() {
		String contents = null;
		try {
			ClipboardService cs = (ClipboardService)ServiceManager.lookup(ClipboardService.class.getName());
			Transferable data = cs.getContents();
			if (data != null && data.isDataFlavorSupported(DataFlavor.stringFlavor)) {
				contents = ((String)(data.getTransferData(DataFlavor.stringFlavor)));
			}
		} catch (Exception e) {
			logger.log(Level.WARNING, "Failed getting the clipboard contents: " + e.getMessage(), e );
		}
		return contents;
	}

	/**
	 * Sets the contents of the system clipboard
	 * 
	 * @param s clipboard contents to set
	 */
	public void setContents(String s) {
		try {
			ClipboardService cs = (ClipboardService)ServiceManager.lookup(ClipboardService.class.getName());
			cs.setContents(new StringSelection(s));
		} catch (Exception e) {
			logger.log(Level.WARNING, "Failed setting the clipboard contents: " + e.getMessage(), e );
		}
	}
}