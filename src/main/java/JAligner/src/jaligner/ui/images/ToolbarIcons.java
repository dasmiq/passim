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

package jaligner.ui.images;

import javax.swing.ImageIcon;

/**
 * Toolbar icons
 * 
 * @author Ahmed Moustafa
 */

public abstract class ToolbarIcons {
    /**
     * Path to the gifs folder 
     */
	public static final String GIFS_HOME = "/jaligner/ui/images/gifs/";
    
    /**
     * Open icon
     */
    public static final ImageIcon OPEN = new ImageIcon(ToolbarIcons.class.getResource(GIFS_HOME + "open.gif"));
    
    /**
     * Save icon
     */
    public static final ImageIcon SAVE = new ImageIcon(ToolbarIcons.class.getResource(GIFS_HOME + "save.gif"));
    
    /**
     * Cut icon
     */
    public static final ImageIcon CUT = new ImageIcon(ToolbarIcons.class.getResource(GIFS_HOME + "cut.gif"));
    
    /**
     * Copy icon
     */
    public static final ImageIcon COPY = new ImageIcon(ToolbarIcons.class.getResource(GIFS_HOME + "copy.gif"));
    
    /**
     * Paste icon
     */
    public static final ImageIcon PASTE = new ImageIcon(ToolbarIcons.class.getResource(GIFS_HOME + "paste.gif"));
    
    /**
     * Delete icon
     */
    public static final ImageIcon DELETE = new ImageIcon(ToolbarIcons.class.getResource(GIFS_HOME + "delete.gif"));
    
    /**
     * Close icon
     */
    public static final ImageIcon CLOSE = new ImageIcon(ToolbarIcons.class.getResource(GIFS_HOME + "close.gif"));
    
    /**
     * About icon
     */
    public static final ImageIcon ABOUT = new ImageIcon(ToolbarIcons.class.getResource(GIFS_HOME + "about.gif"));

	
    /**
     * Print icon
     */
    public static final ImageIcon PRINT = new ImageIcon(ToolbarIcons.class.getResource(GIFS_HOME + "print.gif"));
}