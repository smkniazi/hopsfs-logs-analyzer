package io.hops.experiments;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Results {

  private ConcurrentHashMap<String, Result> map = new ConcurrentHashMap<String, Result>();
  private long count = 0;
  private  long failedOps = 0;

  public Results(){
  }


  public synchronized  void addFailedOp(){
    failedOps++;
  }
  public synchronized  void add(String op, long size, boolean isDir){
    Result res = map.get(op);

    if(res == null){
      res = new Result();
      map.put(op, res);
    }

    res.add(size, isDir);
    count++;
  }

  @Override
  public String toString() {
    String res = "Total Successful Os : "+count+" Failed Ops: "+failedOps+"\n";
    for(String op: map.keySet()){
      Result result = map.get(op);
      res += String.format("%25s", op)+result+"\n";
    }
    return res;
  }

  class Result{
    long total;
    long dirOps;
    Map<Long, Long> stats = new HashMap<Long, Long>();
    Result(){
      stats.put(new Long(1024), new Long(0));
      stats.put(new Long(1024*4), new Long(0));
      stats.put(new Long(1024*5), new Long(0));
      stats.put(new Long(1024*6), new Long(0));
      stats.put(new Long(1024*8), new Long(0));
      stats.put(new Long(1024*16), new Long(0));
      stats.put(new Long(1024*32), new Long(0));
      stats.put(new Long(1024*64), new Long(0));
      stats.put(new Long(1024*100), new Long(0));
      stats.put(new Long(1024*512), new Long(0));
      stats.put(new Long(1024*1024), new Long(0));
      stats.put(new Long(1024*1024*8), new Long(0));
      stats.put(new Long(1024*1024*64), new Long(0));
      stats.put(new Long(1024*1024*256), new Long(0));
      stats.put(new Long(1024*1024*1024), new Long(0));
      stats.put(new Long(1024*1024*1024*128), new Long(0));
      stats.put(new Long(Long.MAX_VALUE), new Long(0));
    }

    public void add(long size, boolean isDir){
      total++;
      if(isDir){
        dirOps++;
        return;
      }

      Set<Long> keys = stats.keySet();
      keys = new TreeSet<Long>(keys);

      for(long key : keys){
        if(size <= key){
          long val = stats.get(key);
          stats.put(key, val+1 );
          return;
        }
      }
    }

    @Override
    public String toString() {
      String res = "";
      Set<Long> keys = stats.keySet();
      keys = new TreeSet<Long>(keys);

      res += String.format("Total %12d dirOps:%12d stats: ", total, dirOps);
      for(long key : keys){
        long val = stats.get(key);
        res  += String.format("%10d",val);
      }
      return  res;
    }
  }
}
