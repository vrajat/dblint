package io.dblint.mart.sqlplanner.utils;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class SelectProvider implements ArgumentsProvider {
  @Override
  public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext)
      throws IOException {
    Stream.Builder<Arguments> argumentsBuilder = Stream.builder();
    for (String filename : extensionContext.getTags()) {
      BufferedReader reader = new BufferedReader(
          new InputStreamReader(this.getClass().getResourceAsStream(filename)));

      String name = null;
      StringBuilder stringBuilder = new StringBuilder();

      Pattern pattern = Pattern.compile("-- ParserTest:(.+)");
      while (reader.ready()) {
        String line = reader.readLine();
        Matcher matcher = pattern.matcher(line);
        if (matcher.matches()) {
          if (name != null) {
            argumentsBuilder.add(Arguments.of(name, stringBuilder.toString()));
          }
          name = matcher.group(1);
          stringBuilder = new StringBuilder();
        } else if (stringBuilder != null) {
          stringBuilder.append(line);
          stringBuilder.append('\n');
        } else {
          throw new IOException("SQL file does not follow required pattern");
        }
      }
      if (name != null) {
        argumentsBuilder.add(Arguments.of(name, stringBuilder.toString()));
      }
    }
    return argumentsBuilder.build();
  }
}
