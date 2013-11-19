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

package jaligner.ui.filechooser;

import jaligner.util.Commons;

/**
 * A factory for {@link jaligner.ui.filechooser.FileChooser}.
 * 
 * @author Ahmed Moustafa
 */

public class FileChooserFactory {
    /**
     * Instance of a concrete {@link FileChooser}.
     */
	private static FileChooser instance = null;
    
	/**
	 * Constructor
	 */
	private FileChooserFactory ( ) {
		super();
	}
	
    /**
     * Returns an instance of a concrete {@link FileChooser}.
     * @return Concrete {@link FileChooser}.
     */
    public static FileChooser getFileChooser( ) {
    	if (instance == null) {
    		 if (Commons.isJnlp()) {
    		 	instance = new FileChooserJNLP();
    		 } else {
    		 	instance = new FileChooserTrusted();
    		 }
    	}
    	return instance;
    }
}