package io.inviscid.mart.server;

public interface Cron {
  long getIterations();

  long getFailedIterations();
}
