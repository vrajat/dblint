package io.dblint.mart.sqlplanner;

public class QanException extends Exception {
  public QanException(String message) {
    super(message);
  }

  public QanException(Throwable throwable) {
    super(throwable);
  }
}
