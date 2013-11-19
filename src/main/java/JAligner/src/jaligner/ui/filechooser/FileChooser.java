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

import java.io.InputStream;

/**
 * Opens and saves files.
 * 
 * @author Ahmed Moustafa
 */

public abstract class FileChooser {
	/**
	 * Buffer size while reading from or write to a file (4 KB) 
	 */
	public static final int BUFFER_SIZE = 4096;
	
	private String userDir = null;
	
	/**
	 * Shows a dialog to select a file
	 * 
	 * @return InputStream
	 * @throws FileChooserException
	 */
	public abstract NamedInputStream open() throws FileChooserException;

	/**
	 * Saves an input stream to a file
	 * 
	 * @param is
	 * @param fileName
	 * @return boolean
	 * @throws FileChooserException
	 */
	public abstract boolean save(InputStream is, String fileName) throws FileChooserException;
	
	/**
	 * Gets the current user working directory
	 * @return current working directory
	 */
	public String getUserDirectory ( ) {
	    if (userDir == null) {
	        userDir = System.getProperty("user.home"); 
	    }
		return userDir;
	}
	
	/**
	 * Sets the user working directory
	 * @param userDir The user directory to set
	 */
	public void setUserDirectory (String userDir) {
	    this.userDir = userDir;
	}
}