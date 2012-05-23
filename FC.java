package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.Random;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import edu.mines.jtk.util.*;

//r = |R|
//n = n
//0.5 = e
//public int k
public class FC {

  public static enum R{
    REMAIN
  }
  public static int r = 100000;
  public static final int n = 100000;
  public static final double e = 0.1;
  public static final int k = 10;

  public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      if(line.charAt(0) == 'S'){
        context.write(new Text("S:"), new Text(line.substring(3)));
      }else if(line.charAt(0) != 'H'){
        Random generator = new Random();
        int randNum = generator.nextInt((int)Math.ceil(r/Math.pow(n,e)));
        context.write(new Text(Integer.toString(randNum)), value);
      }
    }
  }

  public static class Reduce1 extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
      int r = Integer.parseInt(context.getConfiguration().get("r"));
      //context.write(new Text("R:"), new Text(Integer.toString(r)));
      for (Text val : values) {
        String line = val.toString();	
        if(line.charAt(0) == 'S'){
          context.write(new Text("S:"), new Text(line.substring(3)));
        }else{
          Random generator = new Random();
          double randNum = generator.nextDouble();
          if (randNum < 9 * k * Math.pow(n,e) / r * Math.log(n)){
            context.write(new Text("S:"), val);
          }
          randNum = generator.nextDouble();
          if (randNum < 4 * Math.pow(n,e) / r * Math.log(n)){
            context.write(new Text("H:"), val);
          }
        }
      }
    }
  }

  // S/H:point value
  public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line, ":");
      context.write(new Text(tokenizer.nextToken()), new Text(tokenizer.nextToken()));
    }
  } 

  public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
    private HashSet<int[]> h = new HashSet<int[]>();
    private HashSet<int[]> s = new HashSet<int[]>();
    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
      for (Text val : values) {
        String line = val.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        int count = tokenizer.countTokens();
        int points[] = new int[count];
        for(int i = 0; i < count; i++){
          try {
            points[i] = Integer.parseInt(tokenizer.nextToken());
          }
          catch (NumberFormatException ex) {
            if (count>0) count--;
            break;
          }
        }
        if(key.toString().equals("H"))
          h.add(points);
        else
          s.add(points);
      }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

      ConcurrentSkipListMap<Double, int[]> map = new ConcurrentSkipListMap<Double, int[]>();
      Iterator<int[]> iter1 = h.iterator();
      while (iter1.hasNext()){
        int[] p = iter1.next();
        double d = Double.MAX_VALUE;
        Iterator<int[]> iter2 = s.iterator();
        while(iter2.hasNext()){
          int[] q = iter2.next();
          double tmpd = dist(p, q);
          //context.write(new Text("tmpd:"), new Text(Double.toString(tmpd)));
          //context.write(new Text("p:"), new Text(Integer.toString(p[0])));
          //context.write(new Text("q:"), new Text(Integer.toString(q[0])));
          if(tmpd < d)
            d = tmpd;
        }
        map.put(d, p);
        //context.write(new Text("dddd:"), new Text(Double.toString(d)));
      }
      /*
         Iterator<int[]> iter1 = h.iterator();
         while (iter1.hasNext()){
         int[] p = iter1.next();
         List<Double,int[]> set = new Set<int[]>();
         set.add(p);
         set.
         String res = arrayToString(p, " ");
         context.write(new Text("V:"), new Text(res));
         }
         */

      int[][] arr = map.values().toArray(new int[0][]);
      int[] ret = arr[(arr.length - 8 * (int) Math.log(n))>0?(arr.length - 8 * (int) Math.log(n)):0];  /// in fact is 8, not 4
      String res = arrayToString(ret, "");
      context.write(new Text("V:"), new Text(res));

      //context.write(new Text("SSSS:"), new Text(Integer.toString(arr.length)));

      //Iterator<Map.Entry<Double, int[]>> iter = map.entrySet().iterator();

      /*
      //for (int i = 1; i < 8 * Math.log(n); i++) {
      for (int i = n; i >= 8 * Math.log(n); i--) {
      if (iter.hasNext()) {
      iter.next();
      }
      }
      int[] ret;
      if (iter.hasNext()) {
      ret = iter.next().getValue();
      String res = arrayToString(ret, " ");
      context.write(new Text("V:"), new Text(res));
      }
      */

    }

    public static String arrayToString(int[] a, String separator) {
      StringBuffer result = new StringBuffer();
      if (a.length > 0) {
        result.append(Integer.toString(a[0]));
        for (int i=1; i<a.length; i++) {
          result.append(separator);
          result.append(String.valueOf(a[i]));
        }
      }
      return result.toString();
    }
    private double dist(int[] a, int[] b){
      int len = Math.min(a.length, b.length);
      double d = 0;
      for(int i = 0; i < len; i++){
        d += (a[i]-b[i])*(double)(a[i]-b[i]);
      }
      return Math.sqrt(d);
    }
  }

  public static class Map3 extends Mapper<LongWritable, Text, Text, Text> {
    private Text word = new Text();
    private static Random generator = new Random();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      if(line.charAt(0) == 'V' || line.charAt(0) == 'S') {
    	int upper = (int) Math.ceil(Math.pow(n,1-e));
        for(int i = 0; i < upper; i++){
          context.write(new Text(String.valueOf(i)), value);
        }
      }
      else if(line.charAt(0) != 'H'){
        //Random generator = new Random();
        int randNum = generator.nextInt((int) Math.ceil(Math.pow(n,1-e)));
        context.write(new Text(Integer.toString(randNum)), value);
      } 
    }
  } 

  public static class Reduce3 extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
      ArrayList<int[]> s = new ArrayList<int[]>();
      int[] vp = null;
      ArrayList<int[]> r = new ArrayList<int[]>();
      
      RTree rt = new RTree(1, 2, 20);
      ArrayList<RTree.Box> arrbox = new ArrayList<RTree.Box>();
      
      for (Text val : values) {
        String v = val.toString();
        if(v.charAt(0) == 'V'){
          v = v.substring(3);
          StringTokenizer tokenizer = new StringTokenizer(v);
          int count = tokenizer.countTokens();
          vp = new int[count];
          for(int i = 0; i < count; i++){         
            try {
              vp[i] = Integer.parseInt(tokenizer.nextToken());
            }
            catch (NumberFormatException ex) {
              if (count>0) count--;
              break;
            }
          }
        } else
          if(v.charAt(0) == 'S'){
            v = v.substring(3);
            StringTokenizer tokenizer = new StringTokenizer(v);
            int count = tokenizer.countTokens();
            int points[] = new int[count];
            float flpoints[] = new float[count];
            for(int i = 0; i < count; i++){
            	try {
              points[i] = Integer.parseInt(tokenizer.nextToken());
              flpoints[i] = (float) points[i];
            	}
                catch (NumberFormatException ex) {
                  if (count>0) count--;
                  break;
                }
            }
            
            //rt.add(new RTree.Box(flpoints,flpoints));
            arrbox.add(new RTree.Box(flpoints,flpoints));
            s.add(points);

          } else {
            StringTokenizer tokenizer = new StringTokenizer(v);
            int count = tokenizer.countTokens();
            int points[] = new int[count];
            for(int i = 0; i < count; i++){
            	try {
              points[i] = Integer.parseInt(tokenizer.nextToken());
            }
            catch (NumberFormatException ex) {
              if (count>0) count--;
              break;
            }
            }
            r.add(points);
          }
      }
      rt.addPacked(arrbox.toArray());
      
      double d = Double.MAX_VALUE;
      Iterator<int[]> iter = s.iterator();
      while(iter.hasNext()){
        double tmpd = dist(vp, iter.next());
        //context.write(new Text("tmpd"), new Text(String.valueOf(tmpd)));
        if(tmpd < d)
          d = tmpd;
        //context.write(new Text("d"), new Text(String.valueOf(d)));
      }
      Iterator<int[]> iter1 = r.iterator();
      while (iter1.hasNext()){
        double rd = Double.MAX_VALUE;
        int[] p = iter1.next();
        /*Iterator<int[]> iter2 = s.iterator();
        while(iter2.hasNext()){
          double tmpd = dist(p, iter2.next());
          if(tmpd < rd)
            rd = tmpd;
          //context.write(new Text("tmpd_rd"), new Text(String.valueOf(tmpd)));
          //context.write(new Text("rd"), new Text(String.valueOf(rd)));
        }*/
        
        RTree.Box rtn = (RTree.Box) rt.findNearest(int2float(p));
        rd = Math.sqrt(rtn.getDistanceSquared(int2float(p)));
        
        if(rd >= d){
          context.getCounter(R.REMAIN).increment(1);
          String res = arrayToString(p, "");
          context.write(new Text(res), new Text(""));
        }
      }
      //context.write(new Text("V dist"), new Text(String.valueOf(d)));        
      //context.write(new Text("The Key"), key);
    }
    
    private float[] int2float(int[] x) {
    	float y[] = new float[x.length];
    	for (int i=0; i<x.length; i++) {
    		y[i]=(float)x[i];
    	}
    	return y;
    }

    private double dist(int[] a, int[] b){
      int len = Math.min(a.length, b.length);
      double d = 0;
      for(int i = 0; i < len; i++){
        d += (a[i]-b[i])*(double)(a[i]-b[i]);
      }
      return Math.sqrt(d);
    }

    public static String arrayToString(int[] a, String separator) {
      StringBuffer result = new StringBuffer();
      if (a.length > 0) {
        result.append(Integer.toString(a[0]));
        for (int i=1; i<a.length; i++) {
          result.append(separator);
          result.append(String.valueOf(a[i]));
        }
      }
      return result.toString();
    }
  }


  public static class Map4 extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      if(line.charAt(0) == 'S'){
        context.write(new Text("C:"), new Text(line.substring(3)));
      }else if(line.charAt(0) != 'H'){
        context.write(new Text("C:"), value);
      }
    }
  }

  public static class Reduce4 extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
      for (Text val : values) {
        context.write(key, val);
      }
    }
  }

  public static class Map5 extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      if(line.charAt(0) == 'C'){
        for(int i = 0; i < (int)Math.ceil(Math.pow(n,1-e)); i++){
          context.write(new Text(Integer.toString(i)), value);
        }
      }else{
        Random generator = new Random();
        int randNum = generator.nextInt((int)Math.ceil(Math.pow(n,1-e)));
        context.write(new Text(Integer.toString(randNum)), value);
      }
    }
  }

  public static class Reduce5 extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        HashMap<int[], String> c = new HashMap<int[], String>();
        HashSet<int[]> r = new HashSet<int[]>();
        HashMap<String, Integer> count = new HashMap<String, Integer>();
        //RTree rt = new RTree(1, 2, 20);
        for (Text val2 : values) {
          String v = val2.toString();
          if(v.charAt(0) == 'C'){
            v = v.substring(3);
            StringTokenizer tokenizer = new StringTokenizer(v);
            int count2 = tokenizer.countTokens();
            int points[] = new int[count2];
            for(int i = 0; i < count2; i++){
            	
             try {
              points[i] = Integer.parseInt(tokenizer.nextToken());
             }
             catch (NumberFormatException ex) {
               if (count2>0) count2--;
               break;
             }
            }
            c.put(points, v);
            //float flpoints[] = int2float(points);
            //rt.add(new RTree.Box(flpoints,flpoints));

          } else {
            StringTokenizer tokenizer = new StringTokenizer(v);
            int count2 = tokenizer.countTokens();
            int points[] = new int[count2];
            for(int i = 0; i < count2; i++){
            	try {
              points[i] = Integer.parseInt(tokenizer.nextToken());
            }
            catch (NumberFormatException ex) {
              if (count2>0) count2--;
              break;
            }
            }
            r.add(points);
          }
        }

        Iterator<int[]> iter = r.iterator();

        while(iter.hasNext()){
          double d = Double.MAX_VALUE;
          int[] mark = null;
          int[] p = iter.next();
          
          Iterator<int[]> iter2 = c.keySet().iterator();
          while(iter2.hasNext()){
            int[] q = iter2.next();
            double tmpd = dist(p, q);
            if(tmpd <= d){
              d = tmpd;
              mark = q;
            }
          }
          /*
          RTree.Box rtn = (RTree.Box) rt.findNearest(int2float(p));
          if (Math.sqrt(rtn.getDistanceSquared(int2float(p))) <= d) {
        	  float[] tmp = new float[1];
        	  rtn.getBounds(tmp, tmp);
        	  int[] tmpint = float2int(tmp);
        	  mark = tmpint; ///???
          }
          */
          
          if(mark != null){
            String flag = c.get(mark);
            if(count.containsKey(flag)){
              count.put(flag, count.get(flag)+1);
            }else{
              count.put(flag, 1);
            }
          }
        }

        for(String pts: count.keySet()){
          context.write(new Text(Integer.toString(count.get(pts))), new Text(pts));
        }
    }
    private double dist(int[] a, int[] b){
        int len = Math.min(a.length, b.length);
        double d = 0;
        for(int i = 0; i < len; i++){
          d += (a[i]-b[i])*(double)(a[i]-b[i]);
        }
        return Math.sqrt(d);
      }
    private float[] int2float(int[] x) {
    	float y[] = new float[x.length];
    	for (int i=0; i<x.length; i++) {
    		y[i]=(float)x[i];
    	}
    	return y;
    }
    private int[] float2int(float[] x) {
    	int y[] = new int[x.length];
    	for (int i=0; i<x.length; i++) {
    		y[i]=Math.round(x[i]);
    	}
    	return y;
    }

  }

  public static class Map6 extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      String weight = tokenizer.nextToken();
      context.write(new Text(line.substring(weight.length()+1)), new Text(weight));
    }
  }

  public static class Reduce6 extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      int sum = 0;
      for (Text val : values) {
        sum += Integer.parseInt(val.toString());
      }
      context.write(new Text(Integer.toString(sum)), key);
    }
  }

  public static class Reduce7 extends Reducer<Text, Text, Text, Text> {
    HashMap<double[], Integer> c = new HashMap<double[], Integer>();
    HashMap<String, double[]> centers = new HashMap<String, double[]>();
    HashMap<String, double[]> info = new HashMap<String, double[]>();
    HashMap<String, Integer> cc = new HashMap<String, Integer>();

    int dim;
    
    public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      for (Text val : values) {
        String line = key.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        int count = tokenizer.countTokens();
        dim = count;
        double point[] = new double[count];
        for(int i = 0; i < count; i++){
          point[i] = Double.parseDouble(tokenizer.nextToken());
        }
        c.put(point, Integer.parseInt(val.toString()));
      }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
      int s = c.size();
      Random generator = new Random();
      double[] zero = new double[dim];
      for(int i = 0; i < dim; i++)
        zero[i] = 0;
      for(int i = 0; i < FC.k; i++){
        int randNum = generator.nextInt(s);
        centers.put(arrayToString(((double[]) c.keySet().toArray()[randNum])," "), zero);
        cc.put(arrayToString(((double[]) c.keySet().toArray()[randNum]), " "), 0);
        info.put(arrayToString(((double[]) c.keySet().toArray()[randNum]), " "), (double[]) c.keySet().toArray()[randNum]);
      }
      for(String m: centers.keySet()){
        context.write(new Text(m), new Text(""));
      }
      int i = 0;
      while(i++ <= 100){
        for(double[] p : c.keySet()){
          Iterator<String> iter1 = cc.keySet().iterator();
          String mark = null;
          double d = Double.MAX_VALUE;
          while(iter1.hasNext()){
            String q = iter1.next();
            double tmpd = dist(p, info.get(q));
            if(tmpd < d){
              d = tmpd;
              mark = q;
            }
          }
          //context.write(new Text("test" + mark), new Text(""));
          addition(centers.get(mark), p, c.get(p));
          cc.put(mark, cc.get(mark)+c.get(p));
        }
        for(String m: centers.keySet()){
        	
          divide(centers.get(m), cc.get(m));
          //context.write(new Text("2est" + m), new Text(""));
        }
        HashMap<String, double[]> tcenters = new HashMap<String, double[]>();
        HashMap<String, double[]> tinfo = new HashMap<String, double[]>();
        HashMap<String, Integer> tcc = new HashMap<String, Integer>();
        for(String m: centers.keySet()){
        	context.write(new Text(Integer.toString(i) + arrayToString(centers.get(m), " ")), new Text(""));
          tcenters.put(arrayToString(centers.get(m), " "), zero);
          tinfo.put(arrayToString(centers.get(m), " "), centers.get(m));
          tcc.put(arrayToString(centers.get(m), " "), 0);
        }
        centers = tcenters;
        cc = tcc;
        info = tinfo;
      }
      for(String m: centers.keySet()){
        String ret = arrayToString(info.get(m), " ");
        context.write(new Text(ret), new Text(""));
      }
    }

    public static String arrayToString(double[] a, String separator) {
      StringBuffer result = new StringBuffer();
      if (a.length > 0) {
        result.append(Double.toString(a[0]));
        for (int i=1; i<a.length; i++) {
          result.append(separator);
          result.append(String.valueOf(a[i]));
        }
      }
      return result.toString();
    }

    private void addition(double[] a, double[] b, int w){
      int len = Math.min(a.length, b.length);
      for(int i = 0; i < len; i++){
        a[i] += b[i]*w;
      }
    }

    private void divide(double[] a, int w){
      for(int i = 0; i < a.length; i++){
        a[i] = a[i] / w;
      }
    }

    private double dist(double[] a, double[] b){
      int len = Math.min(a.length, b.length);
      double d = 0;
      for(int i = 0; i < len; i++){
        d += (a[i]-b[i])*(double)(a[i]-b[i]);
      }
      return Math.sqrt(d);
    }
  }

  public static void main(String[] args) throws Exception {


    //if (args.length>4) {
    //
    //  FC.r = Integer.parseInt(args[4]);
    //}
	  
	 Configuration conf = new Configuration();
	  
    int i = 1;
    /*
    while(FC.r > 4/e*k*Math.pow(n, e)*Math.log(n)){
      conf.set("r", String.valueOf(FC.r));
      System.out.println(Double.toString(4/e*k*Math.pow(n, e)*Math.log(n)));
      System.out.println(Integer.toString(FC.r));
      String idx1 = Integer.toString(i);
      String idx2 = Integer.toString(++i);

      Job job1 = new Job(conf, "fc");

      job1.setOutputKeyClass(Text.class);
      job1.setOutputValueClass(Text.class);

      job1.setMapperClass(Map1.class);
      job1.setReducerClass(Reduce1.class);

      job1.setInputFormatClass(TextInputFormat.class);
      job1.setOutputFormatClass(TextOutputFormat.class);
      job1.setJarByClass(FC.class);

      FileInputFormat.addInputPath(job1, new Path("input"+idx1));
      FileInputFormat.addInputPath(job1, new Path("output"+idx1));
      FileOutputFormat.setOutputPath(job1, new Path("output"+idx2));

      job1.waitForCompletion(true);

      Job job2 = new Job(conf, "fc");

      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(Text.class);

      job2.setMapperClass(Map2.class);
      job2.setReducerClass(Reduce2.class);

      job2.setInputFormatClass(TextInputFormat.class);
      job2.setOutputFormatClass(TextOutputFormat.class);
      job2.setJarByClass(FC.class);

      FileInputFormat.addInputPath(job2, new Path("output"+idx2));
      FileOutputFormat.setOutputPath(job2, new Path("output"+idx2+idx2));

      job2.waitForCompletion(true);

      Job job3 = new Job(conf, "fc");

      job3.setOutputKeyClass(Text.class);
      job3.setOutputValueClass(Text.class);

      job3.setMapperClass(Map3.class);
      job3.setReducerClass(Reduce3.class);

      job3.setInputFormatClass(TextInputFormat.class);
      job3.setOutputFormatClass(TextOutputFormat.class);
      job3.setJarByClass(FC.class);

      FileInputFormat.addInputPath(job3, new Path("input"+idx1));
      FileInputFormat.addInputPath(job3, new Path("output"+idx2));
      FileInputFormat.addInputPath(job3, new Path("output"+idx2+idx2));
      FileOutputFormat.setOutputPath(job3, new Path("input"+idx2));

      job3.waitForCompletion(true);

      FC.r = (int) job3.getCounters().findCounter(R.REMAIN).getValue();
    }

    Job job4 = new Job(conf, "fc");

    job4.setOutputKeyClass(Text.class);
    job4.setOutputValueClass(Text.class);

    job4.setMapperClass(Map4.class);
    job4.setReducerClass(Reduce4.class);

    job4.setInputFormatClass(TextInputFormat.class);
    job4.setOutputFormatClass(TextOutputFormat.class);
    job4.setJarByClass(FC.class);

    FileInputFormat.addInputPath(job4, new Path("output"+String.valueOf(i)));
    FileInputFormat.addInputPath(job4, new Path("input"+String.valueOf(i)));
    FileOutputFormat.setOutputPath(job4, new Path("c"));

    job4.waitForCompletion(true);


    Job job5 = new Job(conf, "fc");

    job5.setOutputKeyClass(Text.class);
    job5.setOutputValueClass(Text.class);

    job5.setMapperClass(Map5.class);
    job5.setReducerClass(Reduce5.class);

    job5.setInputFormatClass(TextInputFormat.class);
    job5.setOutputFormatClass(TextOutputFormat.class);
    job5.setJarByClass(FC.class);

    FileInputFormat.addInputPath(job5, new Path("input1"));
    FileInputFormat.addInputPath(job5, new Path("c"));
    FileOutputFormat.setOutputPath(job5, new Path("w"));

    job5.waitForCompletion(true);

    Job job6 = new Job(conf, "fc");

    job6.setOutputKeyClass(Text.class);
    job6.setOutputValueClass(Text.class);

    job6.setMapperClass(Map6.class);
    job6.setReducerClass(Reduce6.class);

    job6.setInputFormatClass(TextInputFormat.class);
    job6.setOutputFormatClass(TextOutputFormat.class);
    job6.setJarByClass(FC.class);

    FileInputFormat.addInputPath(job6, new Path("w"));
    FileOutputFormat.setOutputPath(job6, new Path("tw"));

    job6.waitForCompletion(true);
*/
    Job job7 = new Job(conf, "fc");

    job7.setOutputKeyClass(Text.class);
    job7.setOutputValueClass(Text.class);

    job7.setMapperClass(Map6.class);
    job7.setReducerClass(Reduce7.class);

    job7.setInputFormatClass(TextInputFormat.class);
    job7.setOutputFormatClass(TextOutputFormat.class);
    job7.setJarByClass(FC.class);

    FileInputFormat.addInputPath(job7, new Path("tw"));
    FileOutputFormat.setOutputPath(job7, new Path("result"));

    job7.waitForCompletion(true);

  }
}
