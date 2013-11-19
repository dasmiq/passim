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
import java.util.logging.Logger;

import javax.jnlp.FileContents;
import javax.jnlp.FileOpenService;
import javax.jnlp.FileSaveService;
import javax.jnlp.ServiceManager;

/**
 * Opens and saves files.
 * 
 * @author Ahmed Moustafa
 */

public class FileChooserJNLP extends FileChooser {
    private static final Logger logger = Logger.getLogger(FileChooserJNLP.class.getName());
    
	/**
	 * Shows a dialog to select a file.
	 * 
	 * @return InputStream
	 * @throws FileChooserException
	 */
	public NamedInputStream open() throws FileChooserException {
		try {
		    FileOpenService fos = (FileOpenService) ServiceManager.lookup(FileOpenService.class.getName());
			FileContents fc = null;
			if ((fc = fos.openFileDialog(getUserDirectory(), null)) != null) {
			    logger.info("Loaded: " + fc.getName());
				return new NamedInputStream(fc.getName(), fc.getInputStream());
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
	 * @return Boolean
	 * @throws FileChooserException
	 */
	public boolean save(InputStream is, String fileName) throws FileChooserException {
		try {
		    FileSaveService fss = (FileSaveService) ServiceManager.lookup(FileSaveService.class.getName());
		    FileContents fc = fss.saveFileDialog(getUserDirectory(), null, is, fileName);
		    if (fc != null) {
		        logger.info("Saved: " + fc.getName());
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