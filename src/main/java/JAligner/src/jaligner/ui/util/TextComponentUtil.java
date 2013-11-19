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

import jaligner.ui.clipboard.ClipboardHandlerFactory;
import jaligner.ui.filechooser.FileChooserFactory;
import jaligner.ui.filechooser.NamedInputStream;
import jaligner.util.Commons;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.print.Doc;
import javax.print.DocFlavor;
import javax.print.DocPrintJob;
import javax.print.PrintService;
import javax.print.PrintServiceLookup;
import javax.print.ServiceUI;
import javax.print.SimpleDoc;
import javax.print.attribute.HashDocAttributeSet;
import javax.print.attribute.HashPrintRequestAttributeSet;
import javax.print.attribute.PrintRequestAttributeSet;
import javax.print.attribute.standard.DocumentName;
import javax.print.attribute.standard.JobName;
import javax.swing.text.Document;
import javax.swing.text.JTextComponent;

/**
 * Text component (e.g. {@link javax.swing.JTextArea}or
 * {@link javax.swing.JTextPane}) editing helper class.
 * 
 * @author Ahmed Moustafa
 */

public class TextComponentUtil {
    /**
     * Empty string
     */
    private static final String EMPTY = "";

    /**
     * Logger
     */
    private static final Logger logger = Logger
            .getLogger(TextComponentUtil.class.getName());

    /**
     * Copies the selected text to the system clipboad and then deletes it from
     * the text component.
     * 
     * @param textComponent
     */
    public static void cut(JTextComponent textComponent) {
        copy(textComponent);
        delete(textComponent);
    }

    /**
     * Copies the selected text to the system clipboard.
     * 
     * @param textComponent
     */
    public static void copy(JTextComponent textComponent) {
        ClipboardHandlerFactory.getClipboardHandler().setContents(
                textComponent.getSelectedText());
    }

    /**
     * Copies the text contents of the system clipboard to the text component.
     * 
     * @param textComponent
     */
    public static void paste(JTextComponent textComponent) {
        String contents = ClipboardHandlerFactory.getClipboardHandler()
                .getContents();
        if (contents != null) {
            StringBuffer buffer = new StringBuffer();
            char c;
            for (int i = 0, n = contents.length(); i < n; i++) {
                c = contents.charAt(i);
                // Not to copy a null character
                if (c != 0) {
                    buffer.append(c);
                }
            }
            textComponent.replaceSelection(buffer.toString());
        }
        if (!textComponent.hasFocus()) {
            textComponent.requestFocus();
        }
    }

    /**
     * Deletes the selected text in a text component.
     * 
     * @param textComponent
     */
    public static void delete(JTextComponent textComponent) {
        textComponent.replaceSelection(EMPTY);
    }

    /**
     * Selects all contents of a text component.
     * 
     * @param textComponent
     */
    public static void selectAll(JTextComponent textComponent) {
        if (!textComponent.hasFocus()) {
            textComponent.requestFocus();
        }
        textComponent.selectAll();
    }

    /**
     * Opens a file and puts the contents of the file in a text component.
     * 
     * @param textComponent
     * @throws TextComponentUtilException
     */
    public static boolean open(JTextComponent textComponent)
            throws TextComponentUtilException {
        InputStream is = null;
        BufferedReader reader = null;
        try {
            NamedInputStream selectedInputStream = FileChooserFactory
                    .getFileChooser().open();
            if (selectedInputStream != null) {
                is = selectedInputStream.getInputStream();
                textComponent.setText(EMPTY);

                reader = new BufferedReader(new InputStreamReader(is));
                String line;
                Document document = textComponent.getDocument();
                while ((line = reader.readLine()) != null) {
                    document.insertString(document.getLength(), line
                            + Commons.getLineSeparator(), null);
                }
                if (!textComponent.hasFocus()) {
                    textComponent.requestFocus();
                }
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            throw new TextComponentUtilException(e.getMessage());
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (Exception e) {
                    logger.log(Level.WARNING, "Failed closing input stream: "
                            + e.getMessage(), e);
                }
            }
            if (reader != null) {
                try {
                    reader.close();
                } catch (Exception e) {
                    logger.log(Level.WARNING, "Failed closing reader: "
                            + e.getMessage(), e);
                }
            }
        }
    }

    /**
     * Saves the contents of a text component to a file.
     * 
     * @param textComponent
     * @param fileName
     * @return true if a file has been chosen or false otherwise
     * @throws TextComponentUtilException
     */
    public static boolean save(JTextComponent textComponent, String fileName)
            throws TextComponentUtilException {
        InputStream is = null;
        try {
            is = new ByteArrayInputStream(textComponent.getText().getBytes());
            return FileChooserFactory.getFileChooser().save(is, fileName);
        } catch (Exception e) {
            throw new TextComponentUtilException(e.getMessage());
        } finally {
            try {
                is.close();
            } catch (Exception e) {
                logger.log(Level.WARNING, "Failed closing input stream: "
                        + e.getMessage(), e);
            }
        }
    }

    /**
     * Saves the contents of a text component to a file.
     * 
     * @param textComponent
     * @return true if a file has been chosen or false otherwise
     * @throws TextComponentUtilException
     */
    public static boolean save(JTextComponent textComponent)
            throws TextComponentUtilException {
        return save(textComponent, null);
    }

    /**
     * Prints the contents of a text component.
     * 
     * @param textComponent
     */
    public static void print(JTextComponent textComponent)
            throws TextComponentUtilException {
        InputStream is = null;
        try {
            PrintService[] printServices = PrintServiceLookup
                    .lookupPrintServices(DocFlavor.INPUT_STREAM.AUTOSENSE, null);
            if (printServices.length > 0) {

                PrintRequestAttributeSet printRequestAttributeSet = new HashPrintRequestAttributeSet();
                printRequestAttributeSet.add(new JobName("JAligner", null));
                PrintService service = ServiceUI.printDialog(null, 50, 50,
                        printServices, PrintServiceLookup
                                .lookupDefaultPrintService(),
                        DocFlavor.INPUT_STREAM.AUTOSENSE,
                        printRequestAttributeSet);

                if (service != null) {
                    DocPrintJob printJob = service.createPrintJob();
                    PrintJobMointor printJobMointor = new PrintJobMointor(
                            printJob);

                    is = new ByteArrayInputStream(textComponent.getText()
                            .getBytes());

                    DocumentName documentName = new DocumentName("JAligner",
                            null);
                    HashDocAttributeSet docAttributeSet = new HashDocAttributeSet();
                    docAttributeSet.add(documentName);
                    Doc doc = new SimpleDoc(is,
                            DocFlavor.INPUT_STREAM.AUTOSENSE, docAttributeSet);

                    printJob.print(doc, printRequestAttributeSet);
                    printJobMointor.waitForPrintJob();
                }
            } else {
                throw new TextComponentUtilException("No print service found!");
            }
        } catch (Exception e) {
            throw new TextComponentUtilException(e.getMessage());
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    logger.log(Level.WARNING, "Failed closing input stream: "
                            + e.getMessage(), e);
                }
            }
        }
    }
}