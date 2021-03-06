package io.hops.experiments;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class Consumer implements Runnable {
  private ConcurrentLinkedQueue<Operation> operations;
  private AtomicBoolean done;
  private Results res;
  private FileSystem fs = null;

  public Consumer(ConcurrentLinkedQueue<Operation> operations, AtomicBoolean done, Results res) throws IOException {
    this.operations = operations;
    this.done = done;
    this.res = res;
    this.fs = getDFSClient();
  }

  @Override
  public void run() {
    while (true) {
      try {
        if (operations.size() == 0 && done.get() == true) {
          System.out.println("Consumer finished");
          return;
        }

        Operation op = operations.poll();
        if (op != null) {
          getStats(op);
//          randData();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  void getStats(Operation op) throws IOException {
    String path = null;

    if(op.paths[1] != null){
      path = op.paths[1];
    }else{
      path = op.paths[0];
    }

    if(path == null){
      System.err.println("Wrong Path. "+ Arrays.toString(op.paths));
    }

    try {
      FileStatus status = fs.getFileStatus(new Path(path));
      if (status.isDirectory()) {
        res.add(op.opNmae, -1, true);
      } else {
        res.add(op.opNmae, status.getLen(), false);
      }
    } catch (Exception e){
//      System.out.println(e.getMessage());
      res.addFailedOp(op.opNmae);
    }
  }

  Random rand = new Random(System.currentTimeMillis());

  void randData() {
    String[] ops = {"op1", "op2", "op3"};

    String op = ops[rand.nextInt(ops.length)];
    long size = rand.nextInt(1024*1024);
    boolean isDir = rand.nextInt(2) == 0 ? false : true;
    res.add(op, size, isDir);
  }

  public FileSystem getDFSClient() throws IOException {
    return (FileSystem) FileSystem.newInstance(getConf());
//    return null;
  }

  public Configuration getConf() {
    Configuration config = new Configuration();
    config.set("fs.defaultFS", "hdfs://hadoop28:8020");
    return config;
  }
}
