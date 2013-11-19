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



import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.logging.Logger;

import javax.swing.JFileChooser;

/**
 * Opens and saves files.
 * 
 * @author Ahmed Moustafa
 */

public class FileChooserTrusted extends FileChooser {
    private static final Logger logger = Logger.getLogger(FileChooserJNLP.class.getName());
    
	/**
	 * Shows a dialog to select a file.
	 * @return input stream
	 * @throws FileChooserException
	 * @see NamedInputStream
	 */
	public NamedInputStream open() throws FileChooserException {
    	try {
		    JFileChooser chooser = new JFileChooser( );
			chooser.setCurrentDirectory(new File(getUserDirectory()));
		    int returnVal = chooser.showOpenDialog(null);
		    if (returnVal == JFileChooser.APPROVE_OPTION) {
		    	setUserDirectory(chooser.getCurrentDirectory().toString());
		    	logger.info("Loaded: " + chooser.getSelectedFile().getName());
		    	return new NamedInputStream(chooser.getSelectedFile().getName(),
		    								new FileInputStream(chooser.getSelectedFile()));
		    } else {
		    	return null;
		    }
    	} catch (Exception e) {
		    String message = "Failed open: " + e.getMessage();
		    logger.warning(message);
		    throw new FileChooserException(message);
    	}
	}

	/**
	 * Saves an input stream to a file.
	 * 
	 * @param is
	 * @param fileName
	 * @return boolean
	 * @throws FileChooserException
	 */
	public boolean save(InputStream is, String fileName) throws FileChooserException {
		try {
		    JFileChooser chooser = new JFileChooser( );
			chooser.setCurrentDirectory(new File(getUserDirectory()));
			if (fileName != null) {
				chooser.setSelectedFile(new File(fileName));
			}
			int returnVal = chooser.showSaveDialog(null);
			if (returnVal == JFileChooser.APPROVE_OPTION) {
				setUserDirectory(chooser.getCurrentDirectory().toString());
				File file = chooser.getSelectedFile();
				logger.info("Saved: " + file.getName());
				FileOutputStream fos = new FileOutputStream(file);
				byte[] buffer = new byte[BUFFER_SIZE];
				int len;
				while ((len = is.read(buffer)) != -1) {
					fos.write(buffer, 0, len);
				}
				fos.close();
				is.close();
				return true;
			} else {
				return false;
			}
		} catch (Exception e) {
		    String message = "Failed save: " + e.getMessage();
		    logger.warning(message);
		    throw new FileChooserException(message);
		}
	}
}