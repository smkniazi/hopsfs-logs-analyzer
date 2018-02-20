package io.hops.experiments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Producer implements Runnable {


  private List<File> files;
  private ConcurrentLinkedQueue operations;
  private AtomicBoolean done;
  private static Producer instance;

  private Producer() {
    files = null;
    operations = null;
    done = null;
    instance = null;
  }

  private Producer(List<File> files, ConcurrentLinkedQueue operations, AtomicBoolean done) {
    this.files = files;
    this.operations = operations;
    this.done = done;
  }

  public static Producer ProducerFactory(List<File> files, ConcurrentLinkedQueue operations, AtomicBoolean done) {
    if (instance == null) {
      instance = new Producer(files, operations, done);
    } else {
      throw new IllegalStateException("Singleton");
    }
    return instance;
  }

  @Override
  public void run() {
    int lines = 0;
    try {

      for (File file : files) {
        System.out.println("Reading File : " + file);
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        String p1Str = "RequestHandler: [A-Z_]*.*}";
        Pattern p1 = Pattern.compile(p1Str);

        String p2Str = "[A-Z_]+";
        Pattern p2 = Pattern.compile(p2Str);

        String p3Str = "paths=\\[.*\\]";
        Pattern p3 = Pattern.compile(p3Str);

        String line = null;

        while ((line = bufferedReader.readLine()) != null) {
          String opName = null;
          String paths[] = new String[2];

          Matcher m = p1.matcher(line);
          String match1 = null;
          if (m.find()) {
            match1 = m.group(0);
          } else {
            continue;
          }

          line = match1;
          match1 = match1.substring("RequestHandler: ".length());

          Matcher m2 = p2.matcher(match1);
          if (m2.find()) {
            opName = m2.group(0);
          } else {
            continue;
          }


          Matcher m3 = p3.matcher(line);
          String pathstr;
          if (m3.find()) {
            pathstr = m3.group(0).substring("paths=[".length());
            pathstr = pathstr.replace("]", "");
          } else {
            continue;
          }

          if (pathstr.contains(",")) {
            String path1 = pathstr.substring(0, pathstr.indexOf(","));
            String path2 = pathstr.substring(pathstr.indexOf(",") + 2, pathstr.length());
            paths[0] = path1;
            paths[1] = path2;
          } else {
            paths[0] = pathstr;
          }


//          System.out.println("op name " + opName);
//          System.out.println(paths[0]);
//          System.out.println(paths[1]);
//          System.out.println();
          operations.add(new Operation(opName, paths));
          if (operations.size() > 10000) {
            System.out.println("Slowing down producer. To much work in queue");
            Thread.sleep(1000);
          }
          lines++;
        }
        fileReader.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    done.set(true);
    System.out.println("Producer finished. "+lines + " lines read");
    return;
  }
}
