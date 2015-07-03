// BSD License (http://lemurproject.org/galago-license)
package passim;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import java.io.UnsupportedEncodingException;

/**
 * <p>This class processes document text into tokens that can be indexed.</p>
 * 
 * <p>The text is assumed to contain some HTML/XML tags.  The tokenizer tries
 * to extract as much data as possible from each document, even if it is not
 * well formed (e.g. there are start tags with no ending tags).  The resulting
 * document object contains an array of terms and an array of tags.</p> 
 * 
 * @author trevor
 */
public class TagTokenizer {
  
  protected static final boolean[] splits;
  protected static HashSet<String> ignoredTags;
  protected String ignoreUntil;
  protected List<Pattern> whitelist;

  static {
    splits = buildSplits();
    ignoredTags = buildIgnoredTags();
  }
  protected String text;
  protected int position;
  protected int lastSplit;
  ArrayList<String> tokens;
  HashMap<String, ArrayList<BeginTag>> openTags;
  ArrayList<ClosedTag> closedTags;
  ArrayList<Pair> tokenPositions;
  private boolean tokenizeTagContent = true;

  public static class Pair {

    public Pair(int start, int end) {
      this.start = start;
      this.end = end;
    }
    public int start;
    public int end;

    @Override
    public String toString() {
      return String.format("%d,%d", start, end);
    }
  }

  protected enum StringStatus {

    Clean,
    NeedsSimpleFix,
    NeedsComplexFix,
    NeedsAcronymProcessing
  }

  public TagTokenizer() {
    text = null;
    position = 0;
    lastSplit = -1;

    tokens = new ArrayList<String>();
    openTags = new HashMap<String, ArrayList<BeginTag>>();
    closedTags = new ArrayList<ClosedTag>();
    tokenPositions = new ArrayList<Pair>();
    whitelist = new ArrayList<Pattern>();

    whitelist.add(Pattern.compile("pb"));
    whitelist.add(Pattern.compile("w"));
  }

  protected static boolean[] buildSplits() {
    boolean[] localSplits = new boolean[257];

    for (int i = 0; i < localSplits.length; i++) {
      localSplits[i] = false;
    }
    char[] splitChars = {' ', '\t', '\n', '\r', // spaces
      ';', '\"', '&', '/', ':', '!', '#',
      '?', '$', '%', '(', ')', '@', '^',
      '*', '+', '-', ',', '=', '>', '<', '[',
      ']', '{', '}', '|', '`', '~', '_'
    };

    for (char c : splitChars) {
      localSplits[(byte) c] = true;
    }

    for (byte c = 0; c <= 32; c++) {
      localSplits[c] = true;
    }

    return localSplits;
  }

  public void addField(String f) {
    whitelist.add(Pattern.compile(f));
  }

  protected static HashSet<String> buildIgnoredTags() {
    HashSet<String> tags = new HashSet<String>();
    tags.add("style");
    tags.add("script");
    return tags;
  }

  static class ClosedTag {

    public ClosedTag(BeginTag begin, int start, int end) {
      this.name = begin.name;
      this.attributes = begin.attributes;

      this.byteStart = begin.bytePosition;
      this.termStart = begin.termPosition;

      this.byteEnd = start;
      this.termEnd = end;
    }
    String name;
    Map<String, String> attributes;
    int byteStart;
    int termStart;
    int byteEnd;
    int termEnd;
  }

  static class BeginTag {

    public BeginTag(String name, Map<String, String> attributes, int bytePosition, int end) {
      this.name = name;
      this.attributes = attributes;

      this.bytePosition = bytePosition;
      this.termPosition = end;
    }
    String name;
    Map<String, String> attributes;
    int bytePosition;
    int termPosition;
  }

  /**
   * Resets parsing in preparation for the next document.
   */
  public void reset() {
    ignoreUntil = null;
    text = null;
    position = 0;
    lastSplit = -1;

    tokens.clear();
    openTags.clear();
    closedTags.clear();

    if (tokenPositions != null) {
      tokenPositions.clear();
    }
  }

  protected void skipComment() {
    if (text.substring(position).startsWith("<!--")) {
      position = text.indexOf("-->", position + 1);

      if (position >= 0) {
        position += 2;
      }
    } else {
      position = text.indexOf(">", position + 1);
    }

    if (position < 0) {
      position = text.length();
    }
  }

  protected void skipProcessingInstruction() {
    position = text.indexOf("?>", position + 1);

    if (position < 0) {
      position = text.length();
    }
  }

  protected void parseEndTag() {
    // 1. read name (skipping the </ part)
    int i;

    for (i = position + 2; i < text.length(); i++) {
      char c = text.charAt(i);
      if (Character.isSpaceChar(c) || c == '>') {
        break;
      }
    }

    String tagName = text.substring(position + 2, i).toLowerCase();

    if (ignoreUntil != null && ignoreUntil.equals(tagName)) {
      ignoreUntil = null;
    }
    if (ignoreUntil == null) {
      closeTag(tagName);        // advance to end '>'
    }
    while (i < text.length() && text.charAt(i) != '>') {
      i++;
    }
    position = i;
  }

  protected void closeTag(final String tagName) {
    if (!openTags.containsKey(tagName)) {
      return;
    }
    ArrayList<BeginTag> tagList = openTags.get(tagName);

    if (tagList.size() > 0) {
      int last = tagList.size() - 1;

      BeginTag openTag = tagList.get(last);
      ClosedTag closedTag = new ClosedTag(openTag, position, tokens.size());
      closedTags.add(closedTag);

      tagList.remove(last);

      // switch out of Do not tokenize mode.
      if (!tokenizeTagContent) {
        tokenizeTagContent = true;
      }
    }

  }

  protected int indexOfNonSpace(int start) {
    if (start < 0) {
      return Integer.MIN_VALUE;
    }
    for (int i = start; i < text.length(); i++) {
      char c = text.charAt(i);
      if (!Character.isSpaceChar(c)) {
        return i;
      }
    }

    return Integer.MIN_VALUE;
  }

  protected int indexOfEndAttribute(int start, int tagEnd) {
    if (start < 0) {
      return Integer.MIN_VALUE;        // attribute ends at the first non-quoted space, or
      // the first '>'.
    }
    boolean inQuote = false;
    boolean lastEscape = false;

    for (int i = start; i <= tagEnd; i++) {
      char c = text.charAt(i);

      if ((c == '\"' || c == '\'') && !lastEscape) {
        inQuote = !inQuote;
        if (!inQuote) {
          return i;
        }
      } else if (!inQuote && (Character.isSpaceChar(c) || c == '>')) {
        return i;
      } else if (c == '\\' && !lastEscape) {
        lastEscape = true;
      } else {
        lastEscape = false;
      }
    }

    return Integer.MIN_VALUE;
  }

  protected int indexOfSpace(int start) {
    if (start < 0) {
      return Integer.MIN_VALUE;
    }
    for (int i = start; i < text.length(); i++) {
      char c = text.charAt(i);
      if (Character.isSpaceChar(c)) {
        return i;
      }
    }

    return Integer.MIN_VALUE;
  }

  protected int indexOfEquals(int start, int end) {
    if (start < 0) {
      return Integer.MIN_VALUE;
    }
    for (int i = start; i < end; i++) {
      char c = text.charAt(i);
      if (c == '=') {
        return i;
      }
    }

    return Integer.MIN_VALUE;
  }

  protected void parseBeginTag() {
    // 1. read the name, skipping the '<'
    int i;

    for (i = position + 1; i < text.length(); i++) {
      char c = text.charAt(i);
      if (Character.isSpaceChar(c) || c == '>') {
        break;
      }
    }

    String tagName = text.substring(position + 1, i).toLowerCase();

    // 2. read attr pairs
    i = indexOfNonSpace(i);
    int tagEnd = text.indexOf(">", i + 1);
    boolean closeIt = false;

    HashMap<String, String> attributes = new HashMap<String, String>();
    while (i < tagEnd && i >= 0 && tagEnd >= 0) {
      // scan ahead for non space
      int start = indexOfNonSpace(i);

      if (start > 0) {
        if (text.charAt(start) == '>') {
          i = start;
          break;
        } else if (text.charAt(start) == '/'
                && text.length() > start + 1
                && text.charAt(start + 1) == '>') {
          i = start + 1;
          closeIt = true;
          break;
        }
      }

      int end = indexOfEndAttribute(start, tagEnd);
      int equals = indexOfEquals(start, end);

      // try to find an equals sign
      if (equals < 0 || equals == start || end == equals) {
        // if there's no equals, try to move to the next thing
        if (end < 0) {
          i = tagEnd;
          break;
        } else {
          i = end;
          continue;
        }
      }

      // there is an equals, so try to parse the value
      int startKey = start;
      int endKey = equals;

      int startValue = equals + 1;
      int endValue = end;

      if (text.charAt(startValue) == '\"' || text.charAt(startValue) == '\'') {
        startValue++;
      }
      if (startValue >= endValue || startKey >= endKey) {
        i = end;
        continue;
      }

      String key = text.substring(startKey, endKey);
      String value = text.substring(startValue, endValue);

      attributes.put(key.toLowerCase(), value);

      if (end >= text.length()) {
        endParsing();
        break;
      }

      if (text.charAt(end) == '\"' || text.charAt(end) == '\'') {
        end++;
      }

      i = end;
    }

    position = i;

    if (!ignoredTags.contains(tagName)) {
      BeginTag tag = new BeginTag(tagName, attributes, position + 1, tokens.size());

      if (!openTags.containsKey(tagName)) {
        ArrayList tagList = new ArrayList();
        tagList.add(tag);
        openTags.put(tagName, tagList);
      } else {
        openTags.get(tagName).add(tag);
      }

      if (attributes.containsKey("tokenizetagcontent") && !closeIt) {
        String parseAttr = attributes.get("tokenizetagcontent");
        try {
          boolean tokenize = Boolean.parseBoolean(parseAttr);
          tokenizeTagContent = tokenize;
        } catch (Exception e) {
        }
      }

      if (closeIt) {
        closeTag(tagName);
      }
    } else if (!closeIt) {
      ignoreUntil = tagName;
    }

  }

  protected void endParsing() {
    position = text.length();
  }

  public static String processToken(String t) {
    return tokenComplexFix(t);
  }

  protected void onSplit() {
    if (position - lastSplit > 1) {
      int start = lastSplit + 1;
      String token = text.substring(start, position);
      StringStatus status = checkTokenStatus(token);

      switch (status) {
        case NeedsSimpleFix:
          token = tokenSimpleFix(token);
          break;

        case NeedsComplexFix:
          token = tokenComplexFix(token);
          break;

        case NeedsAcronymProcessing:
          tokenAcronymProcessing(token, start, position);
          break;

        case Clean:
          // do nothing
          break;
      }

      if (status != StringStatus.NeedsAcronymProcessing) {
        addToken(token, start, position);
      }
    }

    lastSplit = position;
  }

  public static byte[] StringToBytes(String word) {
    try {
      return word.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("UTF-8 is not supported by your Java Virtual Machine.");
    }
  }
    
  /**
   * Adds a token to the document object.  This method currently drops tokens
   * longer than 100 bytes long right now.
   *
   * @param token  The token to add.
   * @param start  The starting byte offset of the token in the document text.
   * @param end    The ending byte offset of the token in the document text.
   */
  protected void addToken(final String token, int start, int end) {
    final int maxTokenLength = 100;
    // zero length tokens aren't interesting
    if (token.length() <= 0) {
      return;
    }
    // we want to make sure the token is short enough that someone
    // might actually type it.  UTF-8 can expand one character to 6 bytes.
    if (token.length() > maxTokenLength / 6
            && StringToBytes(token).length >= maxTokenLength) {
      return;
    }
    tokens.add(token);
    tokenPositions.add(new Pair(start, end));
  }

  protected static String tokenComplexFix(String token) {
    token = tokenSimpleFix(token);
    token = token.toLowerCase();

    return token;
  }

  /**
   * This method does three kinds of processing:
   * <ul>
   *  <li>If the token contains periods at the beginning or the end,
   *      they are removed.</li>
   *  <li>If the token contains single letters followed by periods, such
   *      as I.B.M., C.I.A., or U.S.A., the periods are removed.</li>
   *  <li>If, instead, the token contains longer strings of text with
   *      periods in the middle, the token is split into
   *      smaller tokens ("umass.edu" becomes {"umass", "edu"}).  Notice
   *      that this means ("ph.d." becomes {"ph", "d"}).</li>
   * </ul>
   *
   * @param token
   * @param start
   * @param end
   */
  protected void tokenAcronymProcessing(String token, int start, int end) {
    token = tokenComplexFix(token);

    // remove start and ending periods
    while (token.startsWith(".")) {
      token = token.substring(1);
      start = start + 1;
    }

    while (token.endsWith(".")) {
      token = token.substring(0, token.length() - 1);
      end -= 1;
    }

    // does the token have any periods left?
    if (token.indexOf('.') >= 0) {
      // is this an acronym?  then there will be periods
      // at odd positions:
      boolean isAcronym = token.length() > 0;
      for (int pos = 1; pos < token.length(); pos += 2) {
        if (token.charAt(pos) != '.') {
          isAcronym = false;
        }
      }

      if (isAcronym) {
        token = token.replace(".", "");
        addToken(token, start, end);
      } else {
        int s = 0;
        for (int e = 0; e < token.length(); e++) {
          if (token.charAt(e) == '.') {
            if (e - s > 1) {
              String subtoken = token.substring(s, e);
              addToken(subtoken, start + s, start + e);
            }
            s = e + 1;
          }
        }

        if (token.length() - s > 1) {
          String subtoken = token.substring(s);
          addToken(subtoken, start + s, end);
        }
      }
    } else {
      addToken(token, start, end);
    }
  }

  /**
   * Scans through the token, removing apostrophes and converting
   * uppercase to lowercase letters.
   *
   * @param token
   * @return
   */
  protected static String tokenSimpleFix(String token) {
    char[] chars = token.toCharArray();
    int j = 0;

    for (int i = 0; i < chars.length; i++) {
      char c = chars[i];
      boolean isAsciiUppercase = (c >= 'A' && c <= 'Z');
      boolean isApostrophe = (c == '\'');

      if (isAsciiUppercase) {
        chars[j] = (char) (chars[i] + 'a' - 'A');
      } else if (isApostrophe) {
        // it's an apostrophe, skip it
        j--;
      } else {
        chars[j] = chars[i];
      }

      j++;
    }

    token = new String(chars, 0, j);
    return token;
  }

  /**
   * This method scans the token, looking for uppercase characters and
   * special characters.  If the token contains only numbers and lowercase
   * letters, it needs no further processing, and it returns Clean.
   * If it also contains uppercase letters or apostrophes, it returns
   * NeedsSimpleFix.  If it contains special characters (especially Unicode
   * characters), it returns NeedsComplexFix.  Finally, if any periods are
   * present, this returns NeedsAcronymProcessing.
   *
   * @param token
   * @return
   */
  protected StringStatus checkTokenStatus(final String token) {
    StringStatus status = StringStatus.Clean;
    char[] chars = token.toCharArray();

    for (int i = 0; i < chars.length; i++) {
      char c = chars[i];
      boolean isAsciiLowercase = (c >= 'a' && c <= 'z');
      boolean isAsciiNumber = (c >= '0' && c <= '9');

      if (isAsciiLowercase || isAsciiNumber) {
        continue;
      }
      boolean isAsciiUppercase = (c >= 'A' && c <= 'Z');
      boolean isPeriod = (c == '.');
      boolean isApostrophe = (c == '\'');

      if ((isAsciiUppercase || isApostrophe) && status == StringStatus.Clean) {
        status = StringStatus.NeedsSimpleFix;
      } else if (!isPeriod) {
        status = StringStatus.NeedsComplexFix;
      } else {
        status = StringStatus.NeedsAcronymProcessing;
        break;
      }
    }

    return status;
  }

  protected void onStartBracket() {
    if (position + 1 < text.length()) {
      char c = text.charAt(position + 1);

      if (c == '/') {
        parseEndTag();
      } else if (c == '!') {
        skipComment();
      } else if (c == '?') {
        skipProcessingInstruction();
      } else {
        parseBeginTag();
      }
    } else {
      endParsing();
    }

    lastSplit = position;
  }

  /**
   * Translates tags from the internal ClosedTag format to the
   * Tag type. Uses the whitelist in the tokenizer to omit tags
   * that are not matched by any patterns in the whitelist
   */
  protected ArrayList<Tag> coalesceTags() {
    ArrayList<Tag> result = new ArrayList();

    // close all open tags
    for (ArrayList<BeginTag> tagList : openTags.values()) {
      for (BeginTag tag : tagList) {
        for (Pattern p : whitelist) {
          if (p.matcher(tag.name).matches()) {
            result.add(new Tag(tag.name, tag.attributes, tag.termPosition, tag.termPosition, tag.bytePosition, tag.bytePosition));
            break;
          }
        }
      }
    }

    for (ClosedTag tag : closedTags) {
      for (Pattern p : whitelist) {
        if (p.matcher(tag.name).matches()) {
          result.add(new Tag(tag.name, tag.attributes, tag.termStart, tag.termEnd, tag.byteStart, tag.byteEnd));
          break;
        }
      }
    }

    Collections.sort(result);
    return result;
  }

  protected void onAmpersand() {
    onSplit();

    for (int i = position + 1; i < text.length(); i++) {
      char c = text.charAt(i);

      if (c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '#') {
        continue;
      }
      if (c == ';') {
        position = i;
        lastSplit = i;
        return;
      }

      // not a valid escape sequence
      break;
    }
  }

  /**
   * Parses the text in the document.text attribute and fills in the
   * document.terms and document.tags arrays.
   *
   * @param document
   */
  public void tokenize(Document document) {
    reset();
    text = document.text;

    try {
      // this loop is looking for tags, split characters, and XML escapes,
      // which start with ampersands.  All other characters are assumed to
      // be word characters.  The onSplit() method takes care of extracting
      // word text and storing it in the terms array.  The onStartBracket
      // method parses tags.  ignoreUntil is used to ignore comments and
      // script data.
      for (; position >= 0 && position < text.length(); position++) {
        char c = text.charAt(position);

        if (c == '<') {
          if (ignoreUntil == null) {
            onSplit();
          }
          onStartBracket();
        } else if (ignoreUntil != null) {
          continue;
        } else if (c == '&') {
          onAmpersand();
        } else if (c < 256 && splits[c] && tokenizeTagContent) {
          onSplit();
        }
      }
    } catch (Exception e) {
      Logger.getLogger(getClass().toString()).log(Level.WARNING,
              "Parse failure: " + document.name);
    }

    if (ignoreUntil == null) {
      onSplit();
    }
    document.terms = new ArrayList<String>(this.tokens);
    for (Pair p : this.tokenPositions) {
      document.termCharBegin.add(p.start);
      document.termCharEnd.add(p.end);
    }
    document.tags = coalesceTags();
  }

  public ArrayList<Pair> getTokenPositions() {
    return this.tokenPositions;
  }
}
