package io.hops.experiments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Producer implements Runnable {


  private final List<File> files;

  public Producer(List<File> files) {
    this.files = files;
  }

  @Override
  public void run() {
    int lines = 0;
    try {
      for (File file : files) {
        System.out.println("Reading File : "+file);
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String pattern_opn = "RequestHandler: [A-Z_]*";
        Pattern r_opn = Pattern.compile(pattern_opn);
        String pattern_paths = "paths=\\[.*\\]";
        Pattern r_paths = Pattern.compile(pattern_paths);
        String line = null;
        String OpName= null;
        String paths[]  = new String[2];
        while ((line = bufferedReader.readLine()) != null) {
          lines++;
          Matcher m = r_opn.matcher(line);
          if (m.find()) {
            OpName = m.group(0).substring("RequestHandler: ".length());
          } else {
            continue;
          }

          System.out.println(OpName);


          m = r_paths.matcher(line);
          String pathstr;
          if (m.find()) {
            pathstr = m.group(0).substring("paths=[".length());
            pathstr = pathstr.replace("]","");
            System.out.println(pathstr);
          } else {
            continue;
          }

          break;
        }
        fileReader.close();
      }
    } catch (IOException e) {
    }
    System.out.println(lines + " lines read");
  }
}
