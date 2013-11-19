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

package jaligner.ui;

import jaligner.Alignment;
import jaligner.Sequence;
import jaligner.SmithWatermanGotoh;
import jaligner.example.SmithWatermanGotohExample;
import jaligner.formats.CLUSTAL;
import jaligner.formats.FASTA;
import jaligner.formats.FormatFactory;
import jaligner.formats.Pair;
import jaligner.matrix.Matrix;
import jaligner.matrix.MatrixLoader;
import jaligner.ui.clipboard.ClipboardListener;
import jaligner.ui.clipboard.ClipboardPoller;
import jaligner.ui.filechooser.FileChooserFactory;
import jaligner.ui.filechooser.NamedInputStream;
import jaligner.ui.images.ToolbarIcons;
import jaligner.ui.logging.DocumentHandler;
import jaligner.ui.util.TextComponentUtil;
import jaligner.ui.util.TextComponentUtilException;
import jaligner.util.Commons;
import jaligner.util.SequenceParser;

import java.awt.Color;
import java.awt.Component;
import java.awt.Cursor;
import java.awt.Image;
import java.awt.event.ActionEvent;
import java.awt.event.MouseEvent;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.swing.AbstractAction;
import javax.swing.AbstractButton;
import javax.swing.Action;
import javax.swing.JComboBox;
import javax.swing.JFormattedTextField;
import javax.swing.JOptionPane;
import javax.swing.JTextArea;
import javax.swing.KeyStroke;
import javax.swing.event.CaretEvent;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.text.JTextComponent;

/**
 * Graphical user inteface for JAligner.
 *
 * @author Ahmed Moustafa
 */

public class AlignWindow extends javax.swing.JFrame implements ClipboardListener, DocumentListener {
    /**
     * 
     */
    private static final long serialVersionUID = 3257844376876364850L;

    /**
     * Window width
     */
    private static final int WINDOW_WIDTH = 800;
    
    /**
     * Window height
     */
    private static final int WINDOW_HEIGHT = 600;
    
    /**
     * Default open gap penalty
     */
    private static final float DEFAULT_OPEN_GAP_PENALTY = 10f;
    
    /**
     * Default extend gap penalty
     */
    private static final float DEFAULT_EXTEND_GAP_PENALTY = 0.5f;
    
    /**
     * Default scoring matrix
     */
    private static final String DEFAULT_SCORING_MATRIX = "BLOSUM62";
    
    /**
     * Logger
     */
    private static final Logger logger = Logger.getLogger(AlignWindow.class.getName());
    
    /**
     * Clipboard poller thread
     */
    private ClipboardPoller clipboardPoller = null;
    
    /**
     * Loaded scoring matrices
     */
    private HashMap<String, Matrix> matrices = new HashMap<String, Matrix>();
    
    /**
     * Current text component
     */
    private JTextComponent currentTextComponent = null;
    
    // The actions
    public Action nextFocusAction = new AbstractAction("Move Focus Forwards") {
        /**
         * 
         */
        private static final long serialVersionUID = 3763091972940183858L;

        public void actionPerformed(ActionEvent evt) {
            ((Component)evt.getSource()).transferFocus();
        }
    };
    
    public Action prevFocusAction = new AbstractAction("Move Focus Backwards") {
        /**
         * 
         */
        private static final long serialVersionUID = 3257844402628997943L;

        public void actionPerformed(ActionEvent evt) {
            ((Component)evt.getSource()).transferFocusBackward();
        }
    };
    
    /**
     * Constructor
     */
    public AlignWindow() {
    	initComponents();

        logger.addHandler(new DocumentHandler(jTextPaneConsole));

        try {
            // Set the icon for the frame
            URL url = getClass().getResource( ToolbarIcons.GIFS_HOME + "jaligner.gif");
            if (url != null) {
                Image image = java.awt.Toolkit.getDefaultToolkit().getImage(url);
                setIconImage(image);
            } else {
                logger.warning("Image URL is NULL");
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed setting the frame image: " + e.getMessage(), e);
        }
        
        jMenuItemFileOpen.setVisible(false);
        
        jFormattedTextFieldGapOpen.setValue(new Float(DEFAULT_OPEN_GAP_PENALTY));
        jFormattedTextFieldGapExtend.setValue(new Float(DEFAULT_EXTEND_GAP_PENALTY));
        
        Collection<String> matrices = null;
        try {
            matrices = MatrixLoader.list(false);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed getting list of scoring matrices: " + e.getMessage(), e);
        }
        if (matrices != null) {
            populateComboBox(jComboBoxScoringMatrix, matrices, DEFAULT_SCORING_MATRIX);
        }
        
        FormatFactory.getInstance().registerFormat(new CLUSTAL());
        FormatFactory.getInstance().registerFormat(new Pair());
        FormatFactory.getInstance().registerFormat(new FASTA());
        
        Collection<String> formats = FormatFactory.getInstance().getFormats();
        String[] outputFormats = new String[formats.size()];
        Iterator<String> i = formats.iterator();
        for (int j = 0; i.hasNext(); j++) {
            outputFormats[j] = i.next();
        }
        populateComboBox(jComboBoxOutputFormat, outputFormats, null);
        
        jTextAreaSequence1.getDocument().addDocumentListener(this);
        jTextAreaSequence2.getDocument().addDocumentListener(this);
        jTextAreaAlignment.getDocument().addDocumentListener(this);
        
        // Add actions
        jTextAreaSequence1.getInputMap().put(KeyStroke.getKeyStroke("TAB"), nextFocusAction.getClass().getName());
        jTextAreaSequence1.getActionMap().put(nextFocusAction.getClass().getName(), nextFocusAction);
        jTextAreaSequence1.getInputMap().put(KeyStroke.getKeyStroke("shift TAB"), prevFocusAction.getClass().getName());
        jTextAreaSequence1.getActionMap().put(prevFocusAction.getClass().getName(), prevFocusAction);
        
        jTextAreaSequence2.getInputMap().put(KeyStroke.getKeyStroke("TAB"), nextFocusAction.getClass().getName());
        jTextAreaSequence2.getActionMap().put(nextFocusAction.getClass().getName(), nextFocusAction);
        jTextAreaSequence2.getInputMap().put(KeyStroke.getKeyStroke("shift TAB"), prevFocusAction.getClass().getName());
        jTextAreaSequence2.getActionMap().put(prevFocusAction.getClass().getName(), prevFocusAction);
        
        // Add radio buttons to group
        buttonGroupSequences.add(jRadioButtonSequence1);
        buttonGroupSequences.add(jRadioButtonSequence2);
        buttonGroupSequences.add(jRadioButtonAlignment);
        buttonGroupSequences.add(jRadioButtonConsole);
        
        // Split the space
        jSplitPaneBody.setResizeWeight(.9D);
        jSplitPaneIO.setResizeWeight(.5D);
        jSplitPaneSequences.setResizeWeight(.5D);
        
        // Set the focus on the text area for sequence #1
        jTextAreaSequence1.requestFocus();
                
        // Start the clipboard checker
        clipboardPoller = new ClipboardPoller(this);
        clipboardPoller.start();
        
        // Hide the print menu item in under the File menu
        jMenuItemFilePrint.setVisible(false);
        
        // Set the frame size
        this.setSize(WINDOW_WIDTH, WINDOW_HEIGHT);
    }
    
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        jPopup = new javax.swing.JPopupMenu();
        jPopupOpen = new javax.swing.JMenuItem();
        jPopupSave = new javax.swing.JMenuItem();
        jPopupSeparator1 = new javax.swing.JSeparator();
        jPopupCut = new javax.swing.JMenuItem();
        jPopupCopy = new javax.swing.JMenuItem();
        jPopupPaste = new javax.swing.JMenuItem();
        jPopupDelete = new javax.swing.JMenuItem();
        jPopupSeparator2 = new javax.swing.JSeparator();
        jPopupPrint = new javax.swing.JMenuItem();
        jPopupSeparator3 = new javax.swing.JSeparator();
        jPopupSelectAll = new javax.swing.JMenuItem();
        buttonGroupSequences = new javax.swing.ButtonGroup();
        jToolBar = new javax.swing.JToolBar();
        jButtonOpen = new javax.swing.JButton();
        jButtonSave = new javax.swing.JButton();
        jButtonCut = new javax.swing.JButton();
        jButtonCopy = new javax.swing.JButton();
        jButtonPaste = new javax.swing.JButton();
        jButtonDelete = new javax.swing.JButton();
        jButtonPrint = new javax.swing.JButton();
        jButtonExit = new javax.swing.JButton();
        jSplitPaneBody = new javax.swing.JSplitPane();
        jSplitPaneIO = new javax.swing.JSplitPane();
        jSplitPaneSequences = new javax.swing.JSplitPane();
        jPanelSequence1 = new javax.swing.JPanel();
        jScrollPaneSequence1 = new javax.swing.JScrollPane();
        jTextAreaSequence1 = new javax.swing.JTextArea();
        jRadioButtonSequence1 = new javax.swing.JRadioButton();
        jPanelSequence2 = new javax.swing.JPanel();
        jScrollPaneSequence2 = new javax.swing.JScrollPane();
        jTextAreaSequence2 = new javax.swing.JTextArea();
        jRadioButtonSequence2 = new javax.swing.JRadioButton();
        jPanelAlignment = new javax.swing.JPanel();
        jRadioButtonAlignment = new javax.swing.JRadioButton();
        jScrollPaneAlignment = new javax.swing.JScrollPane();
        jTextAreaAlignment = new javax.swing.JTextArea();
        jPanelConsole = new javax.swing.JPanel();
        jRadioButtonConsole = new javax.swing.JRadioButton();
        jScrollPaneConsole = new javax.swing.JScrollPane();
        jTextPaneConsole = new javax.swing.JTextPane();
        jPanelControls = new javax.swing.JPanel();
        jPanelScoringMatrix = new javax.swing.JPanel();
        jLabelScoringMatrix = new javax.swing.JLabel();
        jComboBoxScoringMatrix = new javax.swing.JComboBox();
        jPanelGapOpen = new javax.swing.JPanel();
        jLabelOpenGapPenalty = new javax.swing.JLabel();
        jFormattedTextFieldGapOpen = new JFormattedTextField(new DecimalFormat("##0.0##"));
        jPanelGapExtend = new javax.swing.JPanel();
        jLabelExtendGapPenalty = new javax.swing.JLabel();
        jFormattedTextFieldGapExtend = new JFormattedTextField(new DecimalFormat("##0.0##"));
        jPanelOutputFormat = new javax.swing.JPanel();
        jLabelOutputFormat = new javax.swing.JLabel();
        jComboBoxOutputFormat = new javax.swing.JComboBox();
        jPanelGo = new javax.swing.JPanel();
        jButtonGo = new javax.swing.JButton();
        jMenuBar = new javax.swing.JMenuBar();
        jMenuFile = new javax.swing.JMenu();
        jMenuItemFileOpen = new javax.swing.JMenuItem();
        jMenuItemFileLoadSequence1 = new javax.swing.JMenuItem();
        jMenuItemFileLoadSequence2 = new javax.swing.JMenuItem();
        jMenuItemFileLoadMatrix = new javax.swing.JMenuItem();
        jSeparatorFile = new javax.swing.JSeparator();
        jMenuItemFileExit = new javax.swing.JMenuItem();
        jMenuItemFilePrint = new javax.swing.JMenuItem();
        jMenuEdit = new javax.swing.JMenu();
        jMenuItemEditCut = new javax.swing.JMenuItem();
        jMenuItemEditCopy = new javax.swing.JMenuItem();
        jMenuItemEditPaste = new javax.swing.JMenuItem();
        jMenuItemEditDelete = new javax.swing.JMenuItem();
        jMenuItemEditSelectAll = new javax.swing.JMenuItem();
        jMenuTools = new javax.swing.JMenu();
        jMenuItemToolsRunExample = new javax.swing.JMenuItem();
        jMenuHelp = new javax.swing.JMenu();
        jMenuItemAbout = new javax.swing.JMenuItem();

        jPopupOpen.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_O, java.awt.event.InputEvent.CTRL_MASK));
        jPopupOpen.setIcon(ToolbarIcons.OPEN);
        jPopupOpen.setMnemonic('O');
        jPopupOpen.setText("Open...");
        jPopupOpen.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jPopupOpenActionPerformed(evt);
            }
        });
        jPopup.add(jPopupOpen);

        jPopupSave.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_S, java.awt.event.InputEvent.CTRL_MASK));
        jPopupSave.setIcon(ToolbarIcons.SAVE);
        jPopupSave.setMnemonic('S');
        jPopupSave.setText("Save...");
        jPopupSave.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jPopupSaveActionPerformed(evt);
            }
        });
        jPopup.add(jPopupSave);
        jPopup.add(jPopupSeparator1);

        jPopupCut.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_X, java.awt.event.InputEvent.CTRL_MASK));
        jPopupCut.setIcon(ToolbarIcons.CUT);
        jPopupCut.setMnemonic('t');
        jPopupCut.setText("Cut");
        jPopupCut.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jPopupCutActionPerformed(evt);
            }
        });
        jPopup.add(jPopupCut);

        jPopupCopy.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_C, java.awt.event.InputEvent.CTRL_MASK));
        jPopupCopy.setIcon(ToolbarIcons.COPY);
        jPopupCopy.setMnemonic('C');
        jPopupCopy.setText("Copy");
        jPopupCopy.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jPopupCopyActionPerformed(evt);
            }
        });
        jPopup.add(jPopupCopy);

        jPopupPaste.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_V, java.awt.event.InputEvent.CTRL_MASK));
        jPopupPaste.setIcon(ToolbarIcons.PASTE);
        jPopupPaste.setMnemonic('P');
        jPopupPaste.setText("Paste");
        jPopupPaste.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jPopupPasteActionPerformed(evt);
            }
        });
        jPopup.add(jPopupPaste);

        jPopupDelete.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_DELETE, 0));
        jPopupDelete.setIcon(ToolbarIcons.DELETE);
        jPopupDelete.setMnemonic('D');
        jPopupDelete.setText("Delete");
        jPopupDelete.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jPopupDeleteActionPerformed(evt);
            }
        });
        jPopup.add(jPopupDelete);
        jPopup.add(jPopupSeparator2);

        jPopupPrint.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_P, java.awt.event.InputEvent.CTRL_MASK));
        jPopupPrint.setIcon(ToolbarIcons.PRINT);
        jPopupPrint.setMnemonic('D');
        jPopupPrint.setText("Print");
        jPopupPrint.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jPopupPrintActionPerformed(evt);
            }
        });
        jPopup.add(jPopupPrint);
        jPopup.add(jPopupSeparator3);

        jPopupSelectAll.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_A, java.awt.event.InputEvent.CTRL_MASK));
        jPopupSelectAll.setMnemonic('A');
        jPopupSelectAll.setText("Select All");
        jPopupSelectAll.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jPopupSelectAllActionPerformed(evt);
            }
        });
        jPopup.add(jPopupSelectAll);

        setDefaultCloseOperation(javax.swing.WindowConstants.DO_NOTHING_ON_CLOSE);
        setTitle("JAligner - biological pairwise sequence alignment <http://jaligner.sf.net>");
        setName("AlignWindow"); // NOI18N
        addWindowListener(new java.awt.event.WindowAdapter() {
            public void windowClosing(java.awt.event.WindowEvent evt) {
                exitForm(evt);
            }
        });

        jButtonOpen.setIcon(ToolbarIcons.OPEN);
        jButtonOpen.setToolTipText("Open...");
        jButtonOpen.setFocusable(false);
        jButtonOpen.setPreferredSize(new java.awt.Dimension(24, 24));
        jButtonOpen.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButtonOpenActionPerformed(evt);
            }
        });
        jToolBar.add(jButtonOpen);

        jButtonSave.setIcon(ToolbarIcons.SAVE);
        jButtonSave.setToolTipText("Save...");
        jButtonSave.setFocusable(false);
        jButtonSave.setPreferredSize(new java.awt.Dimension(24, 24));
        jButtonSave.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButtonSaveActionPerformed(evt);
            }
        });
        jToolBar.add(jButtonSave);

        jButtonCut.setIcon(ToolbarIcons.CUT);
        jButtonCut.setToolTipText("Cut");
        jButtonCut.setFocusable(false);
        jButtonCut.setPreferredSize(new java.awt.Dimension(24, 24));
        jButtonCut.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButtonCutActionPerformed(evt);
            }
        });
        jToolBar.add(jButtonCut);

        jButtonCopy.setIcon(ToolbarIcons.COPY);
        jButtonCopy.setToolTipText("Copy");
        jButtonCopy.setFocusable(false);
        jButtonCopy.setPreferredSize(new java.awt.Dimension(24, 24));
        jButtonCopy.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButtonCopyActionPerformed(evt);
            }
        });
        jToolBar.add(jButtonCopy);

        jButtonPaste.setIcon(ToolbarIcons.PASTE);
        jButtonPaste.setToolTipText("Paste");
        jButtonPaste.setFocusable(false);
        jButtonPaste.setPreferredSize(new java.awt.Dimension(24, 24));
        jButtonPaste.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButtonPasteActionPerformed(evt);
            }
        });
        jToolBar.add(jButtonPaste);

        jButtonDelete.setIcon(ToolbarIcons.DELETE);
        jButtonDelete.setToolTipText("Delete");
        jButtonDelete.setFocusable(false);
        jButtonDelete.setPreferredSize(new java.awt.Dimension(24, 24));
        jButtonDelete.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButtonDeleteActionPerformed(evt);
            }
        });
        jToolBar.add(jButtonDelete);

        jButtonPrint.setIcon(ToolbarIcons.PRINT);
        jButtonPrint.setToolTipText("Print");
        jButtonPrint.setFocusable(false);
        jButtonPrint.setPreferredSize(new java.awt.Dimension(24, 24));
        jButtonPrint.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButtonPrintActionPerformed(evt);
            }
        });
        jToolBar.add(jButtonPrint);

        jButtonExit.setIcon(ToolbarIcons.CLOSE);
        jButtonExit.setToolTipText("Exit");
        jButtonExit.setFocusable(false);
        jButtonExit.setPreferredSize(new java.awt.Dimension(24, 24));
        jButtonExit.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButtonExitActionPerformed(evt);
            }
        });
        jToolBar.add(jButtonExit);

        getContentPane().add(jToolBar, java.awt.BorderLayout.NORTH);

        jSplitPaneBody.setOrientation(javax.swing.JSplitPane.VERTICAL_SPLIT);
        jSplitPaneBody.setOneTouchExpandable(true);

        jSplitPaneIO.setOneTouchExpandable(true);

        jSplitPaneSequences.setOrientation(javax.swing.JSplitPane.VERTICAL_SPLIT);
        jSplitPaneSequences.setOneTouchExpandable(true);

        jPanelSequence1.setLayout(new java.awt.BorderLayout());

        jScrollPaneSequence1.setPreferredSize(new java.awt.Dimension(400, 50));

        jTextAreaSequence1.setFont(new java.awt.Font("Courier", 0, 12)); // NOI18N
        jTextAreaSequence1.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                jTextAreaSequence1MouseClicked(evt);
            }
        });
        jTextAreaSequence1.addCaretListener(new javax.swing.event.CaretListener() {
            public void caretUpdate(javax.swing.event.CaretEvent evt) {
                jTextAreaSequence1CaretUpdate(evt);
            }
        });
        jTextAreaSequence1.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                jTextAreaSequence1FocusGained(evt);
            }
        });
        jScrollPaneSequence1.setViewportView(jTextAreaSequence1);

        jPanelSequence1.add(jScrollPaneSequence1, java.awt.BorderLayout.CENTER);

        jRadioButtonSequence1.setMnemonic('1');
        jRadioButtonSequence1.setText("Sequence #1");
        jRadioButtonSequence1.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jRadioButtonSequence1ActionPerformed(evt);
            }
        });
        jPanelSequence1.add(jRadioButtonSequence1, java.awt.BorderLayout.NORTH);

        jSplitPaneSequences.setTopComponent(jPanelSequence1);

        jPanelSequence2.setLayout(new java.awt.BorderLayout());

        jScrollPaneSequence2.setPreferredSize(new java.awt.Dimension(400, 50));

        jTextAreaSequence2.setFont(new java.awt.Font("Courier", 0, 12)); // NOI18N
        jTextAreaSequence2.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                jTextAreaSequence2MouseClicked(evt);
            }
        });
        jTextAreaSequence2.addCaretListener(new javax.swing.event.CaretListener() {
            public void caretUpdate(javax.swing.event.CaretEvent evt) {
                jTextAreaSequence2CaretUpdate(evt);
            }
        });
        jTextAreaSequence2.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                jTextAreaSequence2FocusGained(evt);
            }
        });
        jScrollPaneSequence2.setViewportView(jTextAreaSequence2);

        jPanelSequence2.add(jScrollPaneSequence2, java.awt.BorderLayout.CENTER);

        jRadioButtonSequence2.setMnemonic('2');
        jRadioButtonSequence2.setText("Sequence #2");
        jRadioButtonSequence2.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jRadioButtonSequence2ActionPerformed(evt);
            }
        });
        jPanelSequence2.add(jRadioButtonSequence2, java.awt.BorderLayout.NORTH);

        jSplitPaneSequences.setBottomComponent(jPanelSequence2);

        jSplitPaneIO.setLeftComponent(jSplitPaneSequences);

        jPanelAlignment.setLayout(new java.awt.BorderLayout());

        jRadioButtonAlignment.setMnemonic('A');
        jRadioButtonAlignment.setText("Alignment");
        jRadioButtonAlignment.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jRadioButtonAlignmentActionPerformed(evt);
            }
        });
        jPanelAlignment.add(jRadioButtonAlignment, java.awt.BorderLayout.NORTH);

        jScrollPaneAlignment.setPreferredSize(new java.awt.Dimension(400, 50));

        jTextAreaAlignment.setEditable(false);
        jTextAreaAlignment.setFont(new java.awt.Font("Courier", 0, 12)); // NOI18N
        jTextAreaAlignment.setTabSize(0);
        jTextAreaAlignment.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                jTextAreaAlignmentMouseClicked(evt);
            }
        });
        jTextAreaAlignment.addCaretListener(new javax.swing.event.CaretListener() {
            public void caretUpdate(javax.swing.event.CaretEvent evt) {
                jTextAreaAlignmentCaretUpdate(evt);
            }
        });
        jTextAreaAlignment.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                jTextAreaAlignmentFocusGained(evt);
            }
        });
        jScrollPaneAlignment.setViewportView(jTextAreaAlignment);

        jPanelAlignment.add(jScrollPaneAlignment, java.awt.BorderLayout.CENTER);

        jSplitPaneIO.setRightComponent(jPanelAlignment);

        jSplitPaneBody.setTopComponent(jSplitPaneIO);

        jPanelConsole.setLayout(new java.awt.BorderLayout());

        jRadioButtonConsole.setMnemonic('C');
        jRadioButtonConsole.setText("Console");
        jRadioButtonConsole.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jRadioButtonConsoleActionPerformed(evt);
            }
        });
        jPanelConsole.add(jRadioButtonConsole, java.awt.BorderLayout.NORTH);

        jScrollPaneConsole.setPreferredSize(new java.awt.Dimension(400, 50));

        jTextPaneConsole.setEditable(false);
        jTextPaneConsole.setFont(new java.awt.Font("Courier", 0, 12)); // NOI18N
        jTextPaneConsole.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                jTextPaneConsoleMouseClicked(evt);
            }
        });
        jTextPaneConsole.addCaretListener(new javax.swing.event.CaretListener() {
            public void caretUpdate(javax.swing.event.CaretEvent evt) {
                jTextPaneConsoleCaretUpdate(evt);
            }
        });
        jTextPaneConsole.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                jTextPaneConsoleFocusGained(evt);
            }
        });
        jScrollPaneConsole.setViewportView(jTextPaneConsole);

        jPanelConsole.add(jScrollPaneConsole, java.awt.BorderLayout.CENTER);

        jSplitPaneBody.setBottomComponent(jPanelConsole);

        getContentPane().add(jSplitPaneBody, java.awt.BorderLayout.CENTER);

        jPanelControls.setAutoscrolls(true);
        jPanelControls.setLayout(new java.awt.GridLayout(1, 0));

        jLabelScoringMatrix.setDisplayedMnemonic('M');
        jLabelScoringMatrix.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        jLabelScoringMatrix.setLabelFor(jComboBoxScoringMatrix);
        jLabelScoringMatrix.setText("Matrix");
        jPanelScoringMatrix.add(jLabelScoringMatrix);

        jComboBoxScoringMatrix.setToolTipText("Scoring matrix");
        jComboBoxScoringMatrix.setPrototypeDisplayValue("BLOSUMXXX");
        jPanelScoringMatrix.add(jComboBoxScoringMatrix);

        jPanelControls.add(jPanelScoringMatrix);

        jLabelOpenGapPenalty.setDisplayedMnemonic('O');
        jLabelOpenGapPenalty.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        jLabelOpenGapPenalty.setLabelFor(jFormattedTextFieldGapOpen);
        jLabelOpenGapPenalty.setText("Open");
        jPanelGapOpen.add(jLabelOpenGapPenalty);

        jFormattedTextFieldGapOpen.setColumns(3);
        jFormattedTextFieldGapOpen.setToolTipText("Gap open penalty");
        jPanelGapOpen.add(jFormattedTextFieldGapOpen);

        jPanelControls.add(jPanelGapOpen);

        jLabelExtendGapPenalty.setDisplayedMnemonic('E');
        jLabelExtendGapPenalty.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        jLabelExtendGapPenalty.setLabelFor(jFormattedTextFieldGapExtend);
        jLabelExtendGapPenalty.setText("Extend");
        jPanelGapExtend.add(jLabelExtendGapPenalty);

        jFormattedTextFieldGapExtend.setColumns(3);
        jFormattedTextFieldGapExtend.setToolTipText("Gap extend penalty");
        jPanelGapExtend.add(jFormattedTextFieldGapExtend);

        jPanelControls.add(jPanelGapExtend);

        jLabelOutputFormat.setDisplayedMnemonic('F');
        jLabelOutputFormat.setLabelFor(jComboBoxOutputFormat);
        jLabelOutputFormat.setText("Format");
        jPanelOutputFormat.add(jLabelOutputFormat);

        jComboBoxOutputFormat.setToolTipText("Alignment output format");
        jPanelOutputFormat.add(jComboBoxOutputFormat);

        jPanelControls.add(jPanelOutputFormat);

        jButtonGo.setMnemonic('G');
        jButtonGo.setText("Go");
        jButtonGo.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButtonGoActionPerformed(evt);
            }
        });
        jPanelGo.add(jButtonGo);

        jPanelControls.add(jPanelGo);

        getContentPane().add(jPanelControls, java.awt.BorderLayout.SOUTH);

        jMenuFile.setMnemonic('F');
        jMenuFile.setText("File");

        jMenuItemFileOpen.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_O, java.awt.event.InputEvent.CTRL_MASK));
        jMenuItemFileOpen.setIcon(ToolbarIcons.OPEN);
        jMenuItemFileOpen.setMnemonic('O');
        jMenuItemFileOpen.setText("Open...");
        jMenuItemFileOpen.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jMenuItemFileOpenActionPerformed(evt);
            }
        });
        jMenuFile.add(jMenuItemFileOpen);

        jMenuItemFileLoadSequence1.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_1, java.awt.event.InputEvent.CTRL_MASK));
        jMenuItemFileLoadSequence1.setIcon(ToolbarIcons.OPEN);
        jMenuItemFileLoadSequence1.setMnemonic('1');
        jMenuItemFileLoadSequence1.setText("Load sequence #1...");
        jMenuItemFileLoadSequence1.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jMenuItemFileLoadSequence1ActionPerformed(evt);
            }
        });
        jMenuFile.add(jMenuItemFileLoadSequence1);

        jMenuItemFileLoadSequence2.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_2, java.awt.event.InputEvent.CTRL_MASK));
        jMenuItemFileLoadSequence2.setIcon(ToolbarIcons.OPEN);
        jMenuItemFileLoadSequence2.setMnemonic('2');
        jMenuItemFileLoadSequence2.setText("Load sequence #2...");
        jMenuItemFileLoadSequence2.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jMenuItemFileLoadSequence2ActionPerformed(evt);
            }
        });
        jMenuFile.add(jMenuItemFileLoadSequence2);

        jMenuItemFileLoadMatrix.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_M, java.awt.event.InputEvent.CTRL_MASK));
        jMenuItemFileLoadMatrix.setIcon(ToolbarIcons.OPEN);
        jMenuItemFileLoadMatrix.setMnemonic('M');
        jMenuItemFileLoadMatrix.setText("Load scoring matrix...");
        jMenuItemFileLoadMatrix.setToolTipText("Load user-defined scoring matrix from file system");
        jMenuItemFileLoadMatrix.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jMenuItemFileLoadMatrixActionPerformed(evt);
            }
        });
        jMenuFile.add(jMenuItemFileLoadMatrix);
        jMenuFile.add(jSeparatorFile);

        jMenuItemFileExit.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_X, java.awt.event.InputEvent.ALT_MASK));
        jMenuItemFileExit.setIcon(ToolbarIcons.CLOSE);
        jMenuItemFileExit.setMnemonic('X');
        jMenuItemFileExit.setText("Exit");
        jMenuItemFileExit.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jMenuItemFileExitActionPerformed(evt);
            }
        });
        jMenuFile.add(jMenuItemFileExit);

        jMenuItemFilePrint.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_P, java.awt.event.InputEvent.CTRL_MASK));
        jMenuItemFilePrint.setIcon(ToolbarIcons.PRINT);
        jMenuItemFilePrint.setText("Print...");
        jMenuItemFilePrint.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jMenuItemFilePrintActionPerformed(evt);
            }
        });
        jMenuFile.add(jMenuItemFilePrint);

        jMenuBar.add(jMenuFile);

        jMenuEdit.setMnemonic('E');
        jMenuEdit.setText("Edit");

        jMenuItemEditCut.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_X, java.awt.event.InputEvent.CTRL_MASK));
        jMenuItemEditCut.setIcon(ToolbarIcons.CUT);
        jMenuItemEditCut.setMnemonic('t');
        jMenuItemEditCut.setText("Cut");
        jMenuItemEditCut.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jMenuItemEditCutActionPerformed(evt);
            }
        });
        jMenuEdit.add(jMenuItemEditCut);

        jMenuItemEditCopy.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_C, java.awt.event.InputEvent.CTRL_MASK));
        jMenuItemEditCopy.setIcon(ToolbarIcons.COPY);
        jMenuItemEditCopy.setMnemonic('C');
        jMenuItemEditCopy.setText("Copy");
        jMenuItemEditCopy.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jMenuItemEditCopyActionPerformed(evt);
            }
        });
        jMenuEdit.add(jMenuItemEditCopy);

        jMenuItemEditPaste.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_V, java.awt.event.InputEvent.CTRL_MASK));
        jMenuItemEditPaste.setIcon(ToolbarIcons.PASTE);
        jMenuItemEditPaste.setMnemonic('P');
        jMenuItemEditPaste.setText("Paste");
        jMenuItemEditPaste.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jMenuItemEditPasteActionPerformed(evt);
            }
        });
        jMenuEdit.add(jMenuItemEditPaste);

        jMenuItemEditDelete.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_DELETE, 0));
        jMenuItemEditDelete.setIcon(ToolbarIcons.DELETE);
        jMenuItemEditDelete.setMnemonic('D');
        jMenuItemEditDelete.setText("Delete");
        jMenuItemEditDelete.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jMenuItemEditDeleteActionPerformed(evt);
            }
        });
        jMenuEdit.add(jMenuItemEditDelete);

        jMenuItemEditSelectAll.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_A, java.awt.event.InputEvent.CTRL_MASK));
        jMenuItemEditSelectAll.setMnemonic('A');
        jMenuItemEditSelectAll.setText("Select All");
        jMenuItemEditSelectAll.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jMenuItemEditSelectAllActionPerformed(evt);
            }
        });
        jMenuEdit.add(jMenuItemEditSelectAll);

        jMenuBar.add(jMenuEdit);

        jMenuTools.setMnemonic('T');
        jMenuTools.setText("Tools");

        jMenuItemToolsRunExample.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_F5, 0));
        jMenuItemToolsRunExample.setMnemonic('E');
        jMenuItemToolsRunExample.setText("Example");
        jMenuItemToolsRunExample.setToolTipText("Aligns P53 human and P53 mouse");
        jMenuItemToolsRunExample.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jMenuItemToolsRunExampleActionPerformed(evt);
            }
        });
        jMenuTools.add(jMenuItemToolsRunExample);

        jMenuBar.add(jMenuTools);

        jMenuHelp.setMnemonic('H');
        jMenuHelp.setText("Help");

        jMenuItemAbout.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_F1, 0));
        jMenuItemAbout.setIcon(ToolbarIcons.ABOUT);
        jMenuItemAbout.setMnemonic('A');
        jMenuItemAbout.setText("About...");
        jMenuItemAbout.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jMenuItemAboutActionPerformed(evt);
            }
        });
        jMenuHelp.add(jMenuItemAbout);

        jMenuBar.add(jMenuHelp);

        setJMenuBar(jMenuBar);

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void jPopupPrintActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jPopupPrintActionPerformed
        print();
    }//GEN-LAST:event_jPopupPrintActionPerformed

    private void jMenuItemToolsRunExampleActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jMenuItemToolsRunExampleActionPerformed
        try {
            logger.info("Running the example...");
            jTextAreaSequence1.setText(SmithWatermanGotohExample.loadP53Human());
            jTextAreaSequence2.setText(SmithWatermanGotohExample.loadP53Mouse());
            align();
            jTextAreaAlignment.requestFocus();
            logger.info("Finished running the example...");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed running the example: " + e.getMessage(), e);
        }
        
    }//GEN-LAST:event_jMenuItemToolsRunExampleActionPerformed

    private void jMenuItemFilePrintActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jMenuItemFilePrintActionPerformed
        print();
    }//GEN-LAST:event_jMenuItemFilePrintActionPerformed

    private void jButtonPrintActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButtonPrintActionPerformed
        print();
    }//GEN-LAST:event_jButtonPrintActionPerformed

    private void jTextPaneConsoleCaretUpdate(javax.swing.event.CaretEvent evt) {//GEN-FIRST:event_jTextPaneConsoleCaretUpdate
        handleCaretUpdateEvent(evt);
    }//GEN-LAST:event_jTextPaneConsoleCaretUpdate

    private void jTextPaneConsoleMouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_jTextPaneConsoleMouseClicked
        jTextPaneConsole.requestFocus();
        if (evt.getButton() == MouseEvent.BUTTON3) {
            jPopup.show(evt.getComponent(), evt.getX(), evt.getY());
        }
    }//GEN-LAST:event_jTextPaneConsoleMouseClicked

    private void jTextPaneConsoleFocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_jTextPaneConsoleFocusGained
        currentTextComponent = jTextPaneConsole;
        jRadioButtonConsole.setSelected(true);
        handleMoveToTextComponent();
    }//GEN-LAST:event_jTextPaneConsoleFocusGained

    private void jRadioButtonConsoleActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jRadioButtonConsoleActionPerformed
        jTextPaneConsole.requestFocus();
    }//GEN-LAST:event_jRadioButtonConsoleActionPerformed

    private void jTextAreaAlignmentFocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_jTextAreaAlignmentFocusGained
        currentTextComponent = jTextAreaAlignment;
        jRadioButtonAlignment.setSelected(true);
        handleMoveToTextComponent();
    }//GEN-LAST:event_jTextAreaAlignmentFocusGained

    private void jTextAreaSequence2FocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_jTextAreaSequence2FocusGained
        currentTextComponent = jTextAreaSequence2;
        jRadioButtonSequence2.setSelected(true);
        handleMoveToTextComponent();
    }//GEN-LAST:event_jTextAreaSequence2FocusGained

    private void jTextAreaSequence1FocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_jTextAreaSequence1FocusGained
        currentTextComponent = jTextAreaSequence1;
        jRadioButtonSequence1.setSelected(true);
        handleMoveToTextComponent();
    }//GEN-LAST:event_jTextAreaSequence1FocusGained
    
    private void jRadioButtonAlignmentActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jRadioButtonAlignmentActionPerformed
        jTextAreaAlignment.requestFocus();
    }//GEN-LAST:event_jRadioButtonAlignmentActionPerformed
    
    private void jRadioButtonSequence2ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jRadioButtonSequence2ActionPerformed
        jTextAreaSequence2.requestFocus();
    }//GEN-LAST:event_jRadioButtonSequence2ActionPerformed
    
    private void jRadioButtonSequence1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jRadioButtonSequence1ActionPerformed
        jTextAreaSequence1.requestFocus();
    }//GEN-LAST:event_jRadioButtonSequence1ActionPerformed
    
    private void jMenuItemEditCopyActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jMenuItemEditCopyActionPerformed
        copy();
    }//GEN-LAST:event_jMenuItemEditCopyActionPerformed
    
    private void jMenuItemEditDeleteActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jMenuItemEditDeleteActionPerformed
        delete();
    }//GEN-LAST:event_jMenuItemEditDeleteActionPerformed
    
    private void jMenuItemEditPasteActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jMenuItemEditPasteActionPerformed
        paste();
    }//GEN-LAST:event_jMenuItemEditPasteActionPerformed
    
    private void jMenuItemFileOpenActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jMenuItemFileOpenActionPerformed
        loadFileToTextArea("", currentTextComponent);
    }//GEN-LAST:event_jMenuItemFileOpenActionPerformed
    
    private void jMenuItemEditCutActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jMenuItemEditCutActionPerformed
        cut();
    }//GEN-LAST:event_jMenuItemEditCutActionPerformed
    
    private void jMenuItemEditSelectAllActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jMenuItemEditSelectAllActionPerformed
        selectAll();
    }//GEN-LAST:event_jMenuItemEditSelectAllActionPerformed
    
    private void jTextAreaSequence2MouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_jTextAreaSequence2MouseClicked
        jTextAreaSequence2.requestFocus();
        if (evt.getButton() == MouseEvent.BUTTON3) {
            jPopup.show(evt.getComponent(), evt.getX(), evt.getY());
        }
    }//GEN-LAST:event_jTextAreaSequence2MouseClicked
    
    private void jTextAreaSequence2CaretUpdate(javax.swing.event.CaretEvent evt) {//GEN-FIRST:event_jTextAreaSequence2CaretUpdate
        handleCaretUpdateEvent(evt);
    }//GEN-LAST:event_jTextAreaSequence2CaretUpdate
    
    private void jButtonExitActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButtonExitActionPerformed
        exitForm(null);
    }//GEN-LAST:event_jButtonExitActionPerformed
    
    private void jButtonGoActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButtonGoActionPerformed
        align();
    }//GEN-LAST:event_jButtonGoActionPerformed
    
    private void jMenuItemFileLoadSequence2ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jMenuItemFileLoadSequence2ActionPerformed
        loadFileToTextArea("sequence #2", jTextAreaSequence2);
    }//GEN-LAST:event_jMenuItemFileLoadSequence2ActionPerformed
    
    private void jMenuItemFileLoadSequence1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jMenuItemFileLoadSequence1ActionPerformed
        loadFileToTextArea("sequence #1", jTextAreaSequence1);
    }//GEN-LAST:event_jMenuItemFileLoadSequence1ActionPerformed
    
    private void jMenuItemFileLoadMatrixActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jMenuItemFileLoadMatrixActionPerformed
        NamedInputStream selectedInputStream = null;
        try {
            logger.info("Loading scoring matrix...");
            selectedInputStream = FileChooserFactory.getFileChooser().open();
            if (selectedInputStream != null) {
                Matrix matrix = MatrixLoader.load(selectedInputStream);

                // Put the loaded matrix in the matrices hashmap
                matrices.put(selectedInputStream.getName(), matrix);
                
                boolean found = false;
                int index = 0;
                int count = jComboBoxScoringMatrix.getItemCount();
                while (index < count && !found) {
                    if (((String)jComboBoxScoringMatrix.getItemAt(index)).equalsIgnoreCase(selectedInputStream.getName())) {
                        found = true;
                    } else {
                        index++;
                    }
                }
                if (!found) {
                    // Add the loaded matrix to the scoring matrices dropdown menu
                    jComboBoxScoringMatrix.addItem(selectedInputStream.getName());
                    index = jComboBoxScoringMatrix.getItemCount()-1;
                }
                // Set the selected item to the new loaded matrix
                jComboBoxScoringMatrix.setSelectedIndex(index);
                
                logger.info("Finished loading scoring matrix");
            } else {
                logger.info("Canceled loading scoring matrix");
            }
        } catch (Exception e) {
            String message = "Failed loading scoring matrix: " + e.getMessage();
            logger.log(Level.SEVERE, message, e);
            showErrorMessage(message);
        } finally {
            if (selectedInputStream != null) {
                try {
                    selectedInputStream.getInputStream().close();
                } catch(Exception e) {
                    logger.log(Level.WARNING, "Failed closing input stream: " + e.getMessage(), e);
                }
            }
        }
    }//GEN-LAST:event_jMenuItemFileLoadMatrixActionPerformed
    
    private void jTextAreaAlignmentMouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_jTextAreaAlignmentMouseClicked
        jTextAreaAlignment.requestFocus();
        if (evt.getButton() == MouseEvent.BUTTON3) {
            jPopup.show(evt.getComponent(), evt.getX(), evt.getY());
        }
    }//GEN-LAST:event_jTextAreaAlignmentMouseClicked
    
    private void jPopupSaveActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jPopupSaveActionPerformed
        saveTextAreaToFile((JTextArea) jPopup.getInvoker());
    }//GEN-LAST:event_jPopupSaveActionPerformed
    
    private void jButtonSaveActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButtonSaveActionPerformed
        saveTextAreaToFile(currentTextComponent);
    }//GEN-LAST:event_jButtonSaveActionPerformed
    
    private void jTextAreaAlignmentCaretUpdate(javax.swing.event.CaretEvent evt) {//GEN-FIRST:event_jTextAreaAlignmentCaretUpdate
        handleCaretUpdateEvent(evt);
    }//GEN-LAST:event_jTextAreaAlignmentCaretUpdate
    
    private void jPopupOpenActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jPopupOpenActionPerformed
        loadFileToTextArea("", (JTextArea) jPopup.getInvoker());
    }//GEN-LAST:event_jPopupOpenActionPerformed
    
    private void jPopupSelectAllActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jPopupSelectAllActionPerformed
        selectAll();
    }//GEN-LAST:event_jPopupSelectAllActionPerformed
    
    private void jPopupDeleteActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jPopupDeleteActionPerformed
        delete();
    }//GEN-LAST:event_jPopupDeleteActionPerformed
    
    private void jPopupPasteActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jPopupPasteActionPerformed
        paste();
    }//GEN-LAST:event_jPopupPasteActionPerformed
    
    private void jPopupCopyActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jPopupCopyActionPerformed
        copy();
    }//GEN-LAST:event_jPopupCopyActionPerformed
    
    private void jPopupCutActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jPopupCutActionPerformed
        cut();
    }//GEN-LAST:event_jPopupCutActionPerformed
    
    private void jTextAreaSequence1CaretUpdate(javax.swing.event.CaretEvent evt) {//GEN-FIRST:event_jTextAreaSequence1CaretUpdate
        handleCaretUpdateEvent(evt);
    }//GEN-LAST:event_jTextAreaSequence1CaretUpdate
    
    private void jButtonDeleteActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButtonDeleteActionPerformed
        delete();
    }//GEN-LAST:event_jButtonDeleteActionPerformed
    
    private void jButtonPasteActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButtonPasteActionPerformed
        paste();
    }//GEN-LAST:event_jButtonPasteActionPerformed
    
    private void jButtonCopyActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButtonCopyActionPerformed
        copy();
    }//GEN-LAST:event_jButtonCopyActionPerformed
    
    private void jButtonCutActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButtonCutActionPerformed
        cut();
    }//GEN-LAST:event_jButtonCutActionPerformed
    
    private void jButtonOpenActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButtonOpenActionPerformed
        loadFileToTextArea("", currentTextComponent);
    }//GEN-LAST:event_jButtonOpenActionPerformed
    
    private void jTextAreaSequence1MouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_jTextAreaSequence1MouseClicked
        jTextAreaSequence1.requestFocus();
        if (evt.getButton() == MouseEvent.BUTTON3) {
            jPopup.show(evt.getComponent(), evt.getX(), evt.getY());
        }
    }//GEN-LAST:event_jTextAreaSequence1MouseClicked
    
    private void jMenuItemAboutActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jMenuItemAboutActionPerformed
        String message = "JAligner"
                + Commons.getLineSeparator() + Commons.getLineSeparator()
                + "open-source Java implementation"
                + Commons.getLineSeparator()
                + "of the Smith-Waterman algorithm"
                + Commons.getLineSeparator()
                + "for biological sequence alignment."
                + Commons.getLineSeparator() + Commons.getLineSeparator()
                + "Build: " + Commons.getCurrentRelease()
                + Commons.getLineSeparator() + Commons.getLineSeparator()
                + "By: Ahmed Moustafa"
                + Commons.getLineSeparator() + Commons.getLineSeparator()
                + "https://github.com/ahmedmoustafa/JAligner";
                
        JOptionPane.showMessageDialog(this, message, "About JAligner",
                JOptionPane.INFORMATION_MESSAGE);
    }//GEN-LAST:event_jMenuItemAboutActionPerformed
    
    private void jMenuItemFileExitActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jMenuItemFileExitActionPerformed
        exitForm(null);
    }//GEN-LAST:event_jMenuItemFileExitActionPerformed
    
    /** Exit the Application */
    private void exitForm(java.awt.event.WindowEvent evt) {//GEN-FIRST:event_exitForm
        logger.info("Quitting...");
        if (JOptionPane.showConfirmDialog(this, "Are you sure you want to exit JAligner?", "JAligner - confirmation", JOptionPane.YES_NO_OPTION) == JOptionPane.YES_OPTION) {
            logger.info("Thank you for using JAligner!");
            System.exit(0);
        } else {
            logger.info("Canceled quitting");
            currentTextComponent.requestFocus();
        }
    }//GEN-LAST:event_exitForm
    
    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.ButtonGroup buttonGroupSequences;
    private javax.swing.JButton jButtonCopy;
    private javax.swing.JButton jButtonCut;
    private javax.swing.JButton jButtonDelete;
    private javax.swing.JButton jButtonExit;
    private javax.swing.JButton jButtonGo;
    private javax.swing.JButton jButtonOpen;
    private javax.swing.JButton jButtonPaste;
    private javax.swing.JButton jButtonPrint;
    private javax.swing.JButton jButtonSave;
    private javax.swing.JComboBox jComboBoxOutputFormat;
    private javax.swing.JComboBox jComboBoxScoringMatrix;
    private javax.swing.JFormattedTextField jFormattedTextFieldGapExtend;
    private javax.swing.JFormattedTextField jFormattedTextFieldGapOpen;
    private javax.swing.JLabel jLabelExtendGapPenalty;
    private javax.swing.JLabel jLabelOpenGapPenalty;
    private javax.swing.JLabel jLabelOutputFormat;
    private javax.swing.JLabel jLabelScoringMatrix;
    private javax.swing.JMenuBar jMenuBar;
    private javax.swing.JMenu jMenuEdit;
    private javax.swing.JMenu jMenuFile;
    private javax.swing.JMenu jMenuHelp;
    private javax.swing.JMenuItem jMenuItemAbout;
    private javax.swing.JMenuItem jMenuItemEditCopy;
    private javax.swing.JMenuItem jMenuItemEditCut;
    private javax.swing.JMenuItem jMenuItemEditDelete;
    private javax.swing.JMenuItem jMenuItemEditPaste;
    private javax.swing.JMenuItem jMenuItemEditSelectAll;
    private javax.swing.JMenuItem jMenuItemFileExit;
    private javax.swing.JMenuItem jMenuItemFileLoadMatrix;
    private javax.swing.JMenuItem jMenuItemFileLoadSequence1;
    private javax.swing.JMenuItem jMenuItemFileLoadSequence2;
    private javax.swing.JMenuItem jMenuItemFileOpen;
    private javax.swing.JMenuItem jMenuItemFilePrint;
    private javax.swing.JMenuItem jMenuItemToolsRunExample;
    private javax.swing.JMenu jMenuTools;
    private javax.swing.JPanel jPanelAlignment;
    private javax.swing.JPanel jPanelConsole;
    private javax.swing.JPanel jPanelControls;
    private javax.swing.JPanel jPanelGapExtend;
    private javax.swing.JPanel jPanelGapOpen;
    private javax.swing.JPanel jPanelGo;
    private javax.swing.JPanel jPanelOutputFormat;
    private javax.swing.JPanel jPanelScoringMatrix;
    private javax.swing.JPanel jPanelSequence1;
    private javax.swing.JPanel jPanelSequence2;
    private javax.swing.JPopupMenu jPopup;
    private javax.swing.JMenuItem jPopupCopy;
    private javax.swing.JMenuItem jPopupCut;
    private javax.swing.JMenuItem jPopupDelete;
    private javax.swing.JMenuItem jPopupOpen;
    private javax.swing.JMenuItem jPopupPaste;
    private javax.swing.JMenuItem jPopupPrint;
    private javax.swing.JMenuItem jPopupSave;
    private javax.swing.JMenuItem jPopupSelectAll;
    private javax.swing.JSeparator jPopupSeparator1;
    private javax.swing.JSeparator jPopupSeparator2;
    private javax.swing.JSeparator jPopupSeparator3;
    private javax.swing.JRadioButton jRadioButtonAlignment;
    private javax.swing.JRadioButton jRadioButtonConsole;
    private javax.swing.JRadioButton jRadioButtonSequence1;
    private javax.swing.JRadioButton jRadioButtonSequence2;
    private javax.swing.JScrollPane jScrollPaneAlignment;
    private javax.swing.JScrollPane jScrollPaneConsole;
    private javax.swing.JScrollPane jScrollPaneSequence1;
    private javax.swing.JScrollPane jScrollPaneSequence2;
    private javax.swing.JSeparator jSeparatorFile;
    private javax.swing.JSplitPane jSplitPaneBody;
    private javax.swing.JSplitPane jSplitPaneIO;
    private javax.swing.JSplitPane jSplitPaneSequences;
    private javax.swing.JTextArea jTextAreaAlignment;
    private javax.swing.JTextArea jTextAreaSequence1;
    private javax.swing.JTextArea jTextAreaSequence2;
    private javax.swing.JTextPane jTextPaneConsole;
    private javax.swing.JToolBar jToolBar;
    // End of variables declaration//GEN-END:variables
    
    /**
     * Populates a combobox with an array of strings and selects the default
     * @param combobox combobox to be populated
     * @param items array of strings to be added to the combobox
     * @param selected the default selected item
     */
    private void populateComboBox(JComboBox combobox, String[] items, String selected) {
        for (int i = 0; i < items.length; i++) {
            combobox.addItem(items[i]);
            if (items[i].equals(selected)) {
                combobox.setSelectedIndex(i);
            }
        }
    }
    
    /**
     * Populates a combobox with an array of strings and selects the default
     * @param combobox combobox to be populated
     * @param items array of strings to be added to the combobox
     * @param selected the default selected item
     */
    private void populateComboBox(JComboBox combobox, Collection<String> items, String selected) {
        String item;
        Iterator<String> i = items.iterator();
        int index = 0;
        while (i.hasNext()) {
            item = (String) i.next();
            combobox.addItem(item);
            if (item.equals(selected)) {
                combobox.setSelectedIndex(index);
            }
            index++;
        }
    }
    
    /**
     * Displays an error message
     * @param message the error message to be displayed
     */
    private void showErrorMessage(String message) {
        JOptionPane.showMessageDialog(this, message, "JAligner - error message", JOptionPane.ERROR_MESSAGE);
    }
    
    /**
     * Loads the contents of a selected file into a text component.
     * @param id
     * @param textComponent
     */
    private void loadFileToTextArea(String id, JTextComponent textComponent) {
        logger.info("Loading " + id + "...");
        try {
            if (TextComponentUtil.open(textComponent)) {
            	textComponent.requestFocus();
            	textComponent.setCaretPosition(0);
                logger.info("Finished loading " + id);
            } else {
                logger.info("Canceled loading " + id);
            }
        } catch (Exception e) {
            String message = "Failed loading " + id + ": " + e.getMessage();
            logger.log(Level.SEVERE, message, e);
            showErrorMessage(message);
        }
    }
    
    /**
     * Saves a text component to a file.
     * @param textComponent
     */
    private void saveTextAreaToFile(JTextComponent textComponent) {
        try {
            logger.info("Saving...");
            if (TextComponentUtil.save(textComponent)) {
                logger.info("Finished saving");
            } else {
                logger.info("Canceled saving");
            }
        } catch (Exception e) {
            String message = "Failed saving: " + e.getMessage();
            logger.log(Level.SEVERE, message, e);
            showErrorMessage(message);
        }
    }
    
    /**
     *
     *
     */
    private void cut() {
        TextComponentUtil.cut(currentTextComponent);
    }
    
    /**
     *
     *
     */
    private void copy() {
        TextComponentUtil.copy(currentTextComponent);
    }
    
    /**
     *
     *
     */
    private void paste() {
        TextComponentUtil.paste(currentTextComponent);
    }
    
    /**
     *
     *
     */
    private void delete() {
        TextComponentUtil.delete(currentTextComponent);
    }
    
    /**
     * 
     *
     */
    private void selectAll() {
        TextComponentUtil.selectAll(currentTextComponent);
    }
    
    /**
     * Prints the contents of a text component
     *
     */
    private void print() {
    	try {
    		logger.info("Printing...");
    		TextComponentUtil.print(currentTextComponent);
    		logger.info("Finished printing.");
    	} catch (TextComponentUtilException textComponentUtilException) {
    		logger.log(Level.SEVERE, "Failed printing: " + textComponentUtilException.getMessage(), textComponentUtilException);
    	}
    }
    
    /**
     * Implements insertUpdate of {@link DocumentListener}
     */
    public void insertUpdate(DocumentEvent e) {
        if (e.getDocument() == currentTextComponent.getDocument()) {
            setSaveControlsEnabled(true);
            setSelectAllControlsEnabled(true);
            setPrintControlsEnabled(true);
        }
    }
    
    /**
     * Implements removeUpdate of {@link DocumentListener}
     */
    public void removeUpdate(DocumentEvent e) {
        if (e.getDocument() == currentTextComponent.getDocument()) {
            if (e.getDocument().getLength() == 0) {
                setSaveControlsEnabled(false);
                setSelectAllControlsEnabled(false);
                setPrintControlsEnabled(false);
            }
        }
    }
    
    /**
     * Implements changedUpdate of {@link DocumentListener}
     */
    public void changedUpdate(DocumentEvent e) {}
    
    /**
     * Implements the notify method of the interface {@link ClipboardListener}
     */
    public void clipboardCheck(String clipboardContents) {
        setPasteControlsEnabled(clipboardContents != null && currentTextComponent != null && currentTextComponent.isEditable());
    }
    
    /**
     * Sets the status of the open controls
     * @param enabled
     */
    private void setOpenControlsEnabled(boolean enabled) {
        jMenuItemFileOpen.setEnabled(enabled);
        jButtonOpen.setEnabled(enabled);
        jPopupOpen.setEnabled(enabled);
    }
    
    /**
     * Sets the status of the save controls
     * @param enabled
     */
    private void setSaveControlsEnabled(boolean enabled) {
        jButtonSave.setEnabled(enabled);
        jPopupSave.setEnabled(enabled);
    }
    
    /**
     * Sets the status of the cut controls
     * @param enabled
     */
    private void setCutControlsEnabled(boolean enabled) {
        jMenuItemEditCut.setEnabled(enabled);
        jButtonCut.setEnabled(enabled);
        jPopupCut.setEnabled(enabled);
    }
    
    /**
     * Sets the status of the copy controls
     * @param enabled
     */
    private void setCopyControlsEnabled(boolean enabled) {
        jMenuItemEditCopy.setEnabled(enabled);
        jButtonCopy.setEnabled(enabled);
        jPopupCopy.setEnabled(enabled);
    }
    
    /**
     * Sets the status of the paste controls
     * @param enabled
     */
    private void setPasteControlsEnabled(boolean enabled) {
        jMenuItemEditPaste.setEnabled(enabled);
        jButtonPaste.setEnabled(enabled);
        jPopupPaste.setEnabled(enabled);
    }
    
    /**
     * Sets the status of the delete controls
     * @param enabled
     */
    private void setDeleteControlsEnabled(boolean enabled) {
        jMenuItemEditDelete.setEnabled(enabled);
        jButtonDelete.setEnabled(enabled);
        jPopupDelete.setEnabled(enabled);
    }
    
    /**
     * Sets the status of the select all controls
     * @param enabled
     */
    private void setSelectAllControlsEnabled(boolean enabled) {
        jMenuItemEditSelectAll.setEnabled(enabled);
        jPopupSelectAll.setEnabled(enabled);
    }
    
    /**
     * Sets the status of the print controls
     * @param enabled
     */
    private void setPrintControlsEnabled(boolean enabled) {
        jButtonPrint.setEnabled(enabled);
        jPopupPrint.setEnabled(enabled);
    }
    
    /**
     *
     * @param event
     */
    private void handleCaretUpdateEvent(CaretEvent event) {
        if (event.getSource() == currentTextComponent) {
            boolean enabled = event.getDot() != event.getMark();
            // Read controls
            setCopyControlsEnabled(enabled);
            
            // Write controls
            enabled &= currentTextComponent.isEditable();
            setCutControlsEnabled(enabled);
            setDeleteControlsEnabled(enabled);
        }
    }
    
    /**
     *
     *
     */
    private void handleMoveToTextComponent( ) {
        boolean enabled;
        
        enabled = currentTextComponent.getSelectedText() != null;
        setCopyControlsEnabled(enabled);
        
        if (currentTextComponent.isEditable()) {
            setOpenControlsEnabled(true);
            
            setCutControlsEnabled(enabled);
            setDeleteControlsEnabled(enabled);
        } else {
            setPasteControlsEnabled(false);
            
            setOpenControlsEnabled(false);
            
            setCutControlsEnabled(false);
            setDeleteControlsEnabled(false);
        }
        
        enabled = currentTextComponent.getText().length() > 0;
        setSaveControlsEnabled(enabled);
        setSelectAllControlsEnabled(enabled);
        setPrintControlsEnabled(enabled);
        
        // Adjust background of the radio buttons 
        Enumeration<AbstractButton> buttons = buttonGroupSequences.getElements();
        AbstractButton button = null;
        while (buttons.hasMoreElements()) {
            button = (AbstractButton) buttons.nextElement();
            if (button.isSelected()) {
                button.setForeground(Color.blue);
            } else {
                button.setForeground(Color.black);
            }
        }
        
    }
    
    /**
     * 
     *
     */
    private void align() {
        jTextAreaAlignment.setText("");
    	
    	String matrixId = (String) jComboBoxScoringMatrix.getSelectedItem();
        float open = ((Number)jFormattedTextFieldGapOpen.getValue()).floatValue();
        float extend = ((Number)jFormattedTextFieldGapExtend.getValue()).floatValue();
        
        Sequence sequence1 = null;
        try {
            logger.info("Processing sequence #1...");
            sequence1 = SequenceParser.parse(jTextAreaSequence1.getText());
            logger.info("Finished processing sequence #1");
        } catch (Exception e) {
            String message = "Failed parsing sequence #1: " + e.getMessage();
            logger.log(Level.SEVERE, message, e);
            showErrorMessage(message);
            jTextAreaSequence1.requestFocus();
            return;
        }
        
        Sequence sequence2 = null;
        try {
            logger.info("Processing sequence #2...");
            sequence2 = SequenceParser.parse(jTextAreaSequence2.getText());
            logger.info("Finished processing sequence #2");
        } catch (Exception e) {
            String message = "Failed parsing sequence #2: " + e.getMessage();
            logger.log(Level.SEVERE, message, e);
            showErrorMessage(message);
            jTextAreaSequence2.requestFocus();
            return;
        }
        
        logger.info("Aliging...");
        try {
            setCursor(new Cursor(Cursor.WAIT_CURSOR));
            
            long start = System.currentTimeMillis();
            Matrix matrix = null;
            if (!matrices.containsKey(matrixId)) {
                matrix = MatrixLoader.load(matrixId);
                matrices.put(matrixId, matrix);
            } else {
                matrix = (Matrix) matrices.get(matrixId);
            }
            Alignment alignment = SmithWatermanGotoh.align(sequence1, sequence2, matrix, open, extend);
            long end = System.currentTimeMillis();
            logger.info("Finished aligning in " + (end - start) + " milliseconds");
            
            StringBuffer buffer = new StringBuffer();
            
            buffer.append(alignment.getSummary());
            buffer.append(Commons.getLineSeparator());
            
            String formatId = (String) jComboBoxOutputFormat.getSelectedItem();
            String formattedAlignment = FormatFactory.getInstance().getFormat(formatId).format(alignment);
            buffer.append(formattedAlignment);
            
            jTextAreaAlignment.setText("");
            jTextAreaAlignment.append(buffer.toString());
            jTextAreaAlignment.setCaretPosition(0);
        } catch (Error error) {
            String message = "Failed aligning: " + error.getMessage();
            logger.log(Level.SEVERE, message, error);
            showErrorMessage(message);
        } catch (Exception exception) {
            String message = "Failed aligning: " + exception.getMessage();
            logger.log(Level.SEVERE, message, exception);
            showErrorMessage(message);
        } finally {
            setCursor(new Cursor(Cursor.DEFAULT_CURSOR));
        }
    }
    
    /**
     *
     * @param args
     */
    public static void main(String[] args) {
        logger.info( Commons.getJAlignerInfo() );
        new AlignWindow().setVisible(true);
    }
}