package io.hops.experiments;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Results {

  private ConcurrentHashMap<String, Result> map = new ConcurrentHashMap<String, Result>();
  private long count = 0;
  private  long failedOps = 0;

  public Results(){
  }


  public synchronized  void addFailedOp(String op){
    failedOps++;
    Result res = map.get(op);

    if(res == null){
      res = new Result();
      map.put(op, res);
    }
    res.addFailedOp();
  }
  public synchronized  void add(String op, long size, boolean isDir){
    Result res = map.get(op);

    if(res == null){
      res = new Result();
      map.put(op, res);
    }

    res.addSuccOp(size, isDir);
    count++;
  }

  public synchronized void printStats(){
    System.out.println(this.toString());
  }

  @Override
  public String toString() {
    boolean header = true;
    String res = "Total Successful Os : "+count+" Failed Ops: "+failedOps+"\n";
    for(String op: map.keySet()){
      Result result = map.get(op);

      if(header){
        res += String.format("%25s%12s%12s%12s%s", "name", "succOps", "failedOps", "dirOps",result
                .getHeader());
        res+="\n";
        header = false;
      }

      res += String.format("%25s", op)+result+"\n";
    }
    return res;
  }

  class Result{
    long succOps;
    long failedOps;
    long dirOps;
    Map<Long, Long> stats = new HashMap<Long, Long>();
    Result(){
      stats.put(new Long((long)1024), new Long(0));
      stats.put(new Long((long)1024*4), new Long(0));
      stats.put(new Long((long)1024*5), new Long(0));
      stats.put(new Long((long)1024*6), new Long(0));
      stats.put(new Long((long)1024*8), new Long(0));
      stats.put(new Long((long)1024*16), new Long(0));
      stats.put(new Long((long)1024*32), new Long(0));
      stats.put(new Long((long)1024*64), new Long(0));
      stats.put(new Long((long)1024*100), new Long(0));
      stats.put(new Long((long)1024*512), new Long(0));
      stats.put(new Long((long)1024*1024), new Long(0));
      stats.put(new Long((long)1024*1024*8), new Long(0));
      stats.put(new Long((long)1024*1024*64), new Long(0));
      stats.put(new Long((long)1024*1024*256), new Long(0));
      stats.put(new Long((long)1024*1024*1024), new Long(0));
      stats.put(new Long((long)1024*1024*1024*128), new Long(0));
//      stats.put(new Long((long)Long.MAX_VALUE), new Long(0));
    }


    public void addFailedOp(){
      failedOps++;
    }

    public void addSuccOp(long size, boolean isDir){
      succOps++;
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

    public String getHeader(){
      String res = "";
      Set<Long> keys = stats.keySet();
      keys = new TreeSet<Long>(keys);

      for(long key : keys){
        res  += String.format("%12d",key/1024);
      }

      return res;
    }

    @Override
    public String toString() {
      String res = "";
      res += String.format("%12d%12d%12d", succOps, failedOps, dirOps);
      Set<Long> keys = stats.keySet();
      keys = new TreeSet<Long>(keys);
      for(long key : keys){
        long val = stats.get(key);
        res  += String.format("%12d",val);
      }
      return  res;
    }
  }
}
