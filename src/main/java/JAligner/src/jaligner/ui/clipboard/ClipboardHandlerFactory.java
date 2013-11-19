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

import jaligner.util.Commons;

/**
 * A factory for {@link jaligner.ui.clipboard.ClipboardHandler}.
 * 
 * @author Ahmed Moustafa
 */

public class ClipboardHandlerFactory {
    /**
     * Instance of a concrete {@link ClipboardHandlerFactory}.
     */
	private static ClipboardHandler instance = null;
    
	/**
	 * Constructor
	 */
	private ClipboardHandlerFactory ( ) {
		super();
	}
	
    /**
     * Returns an instance of a concrete {@link ClipboardHandler}.
     * @return Concrete {@link ClipboardHandler}.
     */
    public static ClipboardHandler getClipboardHandler( ) {
    	if (instance == null) {
    		 if (Commons.isJnlp()) {
    		 	instance = new ClipboardHandlerJNLP();
    		 } else {
    		 	instance = new ClipboardHandlerAWT();
    		 }
    	}
    	return instance;
    }
}