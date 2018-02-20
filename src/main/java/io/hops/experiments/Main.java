package io.hops.experiments;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class Main {
  @Option(name = "-path", usage = "Location of log files")
  private static String path = "/home/salman/logs" ;

  private static ThreadLocal<FileSystem> dfsClients = new ThreadLocal<FileSystem>();
  ConcurrentLinkedQueue<Operation> operations = new ConcurrentLinkedQueue<Operation>();


  public static void main(String argv[]) throws InterruptedException {
    (new Main()).start(argv);
  }

  private void parseArgs(String[] args) {
    CmdLineParser parser = new CmdLineParser(this);
    parser.setUsageWidth(80);
    try {
      // parse the arguments.
      parser.parseArgument(args);
    } catch (Exception e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      System.err.println();
      System.exit(-1);
    }

  }

  public static List<File> findFiles(String path) {
    List<File> allResultFiles = new ArrayList<File>();
    File root = new File(path);
    if (!root.isDirectory()) {
      System.err.println(path + " is not a directory. Specify a directory that contains all the results");
      return allResultFiles;
    }

    List<File> dirs = new ArrayList<File>();
    dirs.add(root);
    while (!dirs.isEmpty()) {
      File dir = dirs.remove(0);

      File[] contents = dir.listFiles();
      if (contents != null && contents.length > 0) {
        for (File content : contents) {
          if (content.isDirectory()) {
            dirs.add(content);
          } else {
              System.out.println("Found a file  " + content.getAbsolutePath());
              allResultFiles.add(content);
            }
          }
        }
      }
    return allResultFiles;
  }

  public void start(String argv[]) throws InterruptedException {
    parseArgs(argv);
    Producer prod = new Producer(findFiles(path));
    Thread t = new Thread(prod);
    t.start();
    t.join();
    return;
  }


  public static FileSystem getDFSClient(Configuration conf) throws IOException {
    FileSystem client = dfsClients.get();
    if (client == null) {
      client = (FileSystem) FileSystem.newInstance(conf);
      dfsClients.set(client);
    }else{
      System.out.println("Reusing Existing Client "+client);
    }
    return client;
  }
}


