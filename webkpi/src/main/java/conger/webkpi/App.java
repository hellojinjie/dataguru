package conger.webkpi;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hello world!
 * 
 */
public class App {
  public static void main(String[] args) throws Exception {
    Pattern pattern = Pattern.compile("([^ ]*) ([^ ]*) ([^ ]*) (-|\\[[^\\]]*\\]) ([^ \"]*|\"[^\"]*\") (-|[0-9]*) (-|[0-9]*) ([^ \"]*|\"[^\"]*\") ([^ \"]*|\"[^\"]*\")");
    BufferedReader br = new BufferedReader(new FileReader(new File("/home/jj/tmp/access.small.log")));
    String oneLine = br.readLine();
    oneLine = br.readLine();
    Matcher matcher = pattern.matcher(oneLine);
    if (matcher.matches()) {
      int count = matcher.groupCount();
      for (int i = 1; i <= count; i++) {
        System.out.println("" + i + " " + matcher.group(i));
      }
    }
    br.close();
  }
}
