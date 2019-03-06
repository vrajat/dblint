package io.dblint.mart.metricsink.mysql;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;

public class RewindBufferedReader extends LineNumberReader {
  String line;

  RewindBufferedReader(Reader reader) {
    super(reader);
  }

  void rewind(String line) {
    this.line = line;
    super.setLineNumber(super.getLineNumber() - 1);
  }

  @Override
  public String readLine() throws IOException {
    if (this.line != null) {
      String rewound = line;
      line = null;
      super.setLineNumber(super.getLineNumber() + 1);
      return rewound;
    }

    return super.readLine();
  }
}
