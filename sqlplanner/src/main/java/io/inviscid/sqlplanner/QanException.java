package io.inviscid.sqlplanner;

public class QanException extends Exception {
  public QanException(String message) {
    super(message);
  }

  public QanException(Throwable throwable) {
    super(throwable);
  }
}
