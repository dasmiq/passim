// BSD License (http://lemurproject.org/galago-license)
package passim;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Document {

  static final int BUFFER_SIZE = 5000;
  // document id - this value is serialized
  public long identifier = -1;
  // document data - these values are serialized
  public String name;
  public Map<String, String> metadata;
  public String text;
  public List<String> terms;
  public List<Integer> termCharBegin = new ArrayList<>();
  public List<Integer> termCharEnd = new ArrayList<>();
  public List<Tag> tags;
  // other data - used to generate an identifier; these values can not be serialized!
  public int fileId = -1;
  public int totalFileCount = -1;
  public String filePath = "";
  public long fileLocation = -1;

  public Document() {
    metadata = new HashMap<>();
  }

  public Document(String externalIdentifier, String text) {
    this();
    this.name = externalIdentifier;
    this.text = text;
  }

  public Document(Document d) {
    this.identifier = d.identifier;
    this.name = d.name;
    this.metadata = new HashMap<>(d.metadata);
    this.text = d.text;
    this.terms = new ArrayList<>(d.terms);
    this.termCharBegin = new ArrayList<>(d.termCharBegin);
    this.termCharEnd = new ArrayList<>(d.termCharEnd);
    this.tags = new ArrayList<>(d.tags);
    this.fileId = d.fileId;
    this.totalFileCount = d.totalFileCount;
  }
}
