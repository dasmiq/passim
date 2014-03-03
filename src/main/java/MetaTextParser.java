// BSD License (http://lemurproject.org/galago-license)
package passim;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.DocumentStreamParser;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author dasmith
 */
public class MetaTextParser extends DocumentStreamParser {

  BufferedReader reader;

  /**
   * Creates a new instance of MetaTextParser
   */
  public MetaTextParser(DocumentSplit split, Parameters p) throws FileNotFoundException, IOException {
    super(split, p);
    this.reader = getBufferedReader(split);
  }

  public String waitFor(String tag) throws IOException {
    String line;

    while ((line = reader.readLine()) != null) {
      if (line.startsWith(tag)) {
        return line;
      }
    }

    return null;
  }

  public String parseDocNumber() throws IOException {
    String allText = waitFor("<docno>");
    if (allText == null) {
      return null;
    }

    while (allText.contains("</docno>") == false) {
      String line = reader.readLine();
      if (line == null) {
        break;
      }
      allText += line;
    }

    int start = allText.indexOf("<docno>") + 7;
    int end = allText.indexOf("</docno>");

    return new String(allText.substring(start, end).trim());
  }

  public Document nextDocument() throws IOException {
    String line;

    Document doc = new Document();

    if (reader == null || null == waitFor("<doc>")) {
      return null;
    }
    String identifier = parseDocNumber();
    if (identifier == null) {
      return null;
    }
    doc.name = identifier;

    StringBuilder buffer = new StringBuilder();

    String inTag = "";

    while ((line = reader.readLine()) != null) {
      if (line.startsWith("</doc>")) {
	  break;
      }
      if (line.startsWith("<")) {
	if (inTag.equals("text") && line.startsWith("</text>")) {
	  inTag = "";

          buffer.append(line);
          buffer.append('\n');
        } else if (inTag.equals("")) {
	  if (line.startsWith("<text>")) {
	    inTag = "text";
	  } else {
	      int start = line.indexOf('>');
	      int end = line.lastIndexOf('<');
	      doc.metadata.put(line.substring(1, start),
			       new String(line.substring(start+1, end).trim()));
	  }
        }
      }

      if (!inTag.equals("")) {
        buffer.append(line);
        buffer.append('\n');
      }
    }
    doc.text = buffer.toString();
    return doc;
  }

  @Override
  public void close() throws IOException {
    if (this.reader != null) {
      this.reader.close();
      this.reader = null;
    }
  }
}
