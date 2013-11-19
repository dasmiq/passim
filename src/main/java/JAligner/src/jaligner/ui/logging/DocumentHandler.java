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

package jaligner.ui.logging;

import java.awt.Color;
import java.util.logging.ErrorManager;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import javax.swing.JTextPane;
import javax.swing.text.Style;
import javax.swing.text.StyleConstants;
import javax.swing.text.StyledDocument;

/**
 * Logging handler of {@link javax.swing.JTextPane}.
 * 
 * @author Ahmed Moustafa
 */

public class DocumentHandler extends Handler {
	/**
	 * Maximum document size
	 */
	private static final int MAXIMUM_DOCUMENT_SIZE = 524288; // 0.5 MB
	
	private JTextPane textPane = null;
	private Style infoStyle = null;
	private Style severStyle = null;

	/**
	 * Constructor
	 * @param textPane
	 */
	public DocumentHandler(JTextPane textPane) {
        this.textPane = textPane;
       	setFormatter(new RecordFormatter());

       	StyledDocument document = (StyledDocument) this.textPane.getDocument();
        
       	infoStyle = document.addStyle("INFO", null);
        StyleConstants.setFontFamily(infoStyle,"Monospaced");
        StyleConstants.setBackground(infoStyle, Color.white);
		StyleConstants.setForeground(infoStyle, Color.blue);
        
		severStyle = document.addStyle("SEVER", null);
        StyleConstants.setFontFamily(severStyle,"Monospaced");
        StyleConstants.setBackground(severStyle, Color.white);
		StyleConstants.setForeground(severStyle, Color.red);
    }

	
    /**
     * 
     */
	public void close() {
    }

    /**
     * 
     */
    public void flush() {
    }
    
    /**
     * 
     */
    public void publish(LogRecord record) {
        if (!isLoggable(record)) {
            return;
        }
        
        String message;
        
        try {
        	message = getFormatter().format(record);
        } catch (Exception exception) {
            reportError(null, exception, ErrorManager.FORMAT_FAILURE);
            return;
        }
        
        synchronized(textPane) {
        	if (textPane.getDocument().getLength() >= MAXIMUM_DOCUMENT_SIZE) {
        		// Delete the contents of the text pane.
        		textPane.setText("");
        	}
        	
        	try {
       	        if (record.getLevel() == Level.SEVERE) {
       	        	textPane.getDocument().insertString(textPane.getDocument().getLength(), message, severStyle);
        		} else {
        			textPane.getDocument().insertString(textPane.getDocument().getLength(), message, infoStyle);
        		}
       	        textPane.setCaretPosition(textPane.getDocument().getLength());
        	} catch (Exception ex) {
        		reportError(null, ex, ErrorManager.WRITE_FAILURE);
        	}
        }
        
	}
}