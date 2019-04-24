package io.dblint.mart.server.commands;

import io.dblint.mart.server.commands.mysql.AnalyzeSlowQuery;
import io.dblint.mart.server.commands.mysql.ErrorLog;
import io.dblint.mart.server.commands.mysql.InnodbLockWaitLog;
import io.dblint.mart.server.commands.mysql.LongTxnLog;
import io.dblint.mart.server.commands.mysql.SlowQueryLog;

import java.util.SortedMap;
import java.util.TreeMap;

public class MySqlCommands extends HierarchicalCommand {
  private static SortedMap<String, Command> generateSubCommands() {
    final SortedMap<String, Command> commands = new TreeMap<>();
    final SlowQueryLog slowQueryLog = new SlowQueryLog();
    commands.put(slowQueryLog.getName(), slowQueryLog);

    final ErrorLog errorLog = new ErrorLog();
    commands.put(errorLog.getName(), errorLog);

    final InnodbLockWaitLog innodbLockWaitLog = new InnodbLockWaitLog();
    commands.put(innodbLockWaitLog.getName(), innodbLockWaitLog);

    final LongTxnLog longTxnLog = new LongTxnLog();
    commands.put(longTxnLog.getName(), longTxnLog);

    final AnalyzeSlowQuery analyzeSlowQuery = new AnalyzeSlowQuery();
    commands.put(analyzeSlowQuery.getName(), analyzeSlowQuery);

    return commands;
  }

  public MySqlCommands() {
    super("mysql", "MySQL Commands", generateSubCommands());
  }
}
