package exp2;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.Vector;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
 
public class InvertedIndex {
	
	public static class Map extends Mapper<Object, Text, Text, IntWritable> {
		/**
		 * setup():读取停词表到vector stop_words中
		 */
        Vector<String> stop_words;//停词表
        protected void setup(Context context) throws IOException {
        	stop_words = new Vector<String>();//初始化停词表
        	Configuration conf = context.getConfiguration();
        	//读取停词表文件
        	BufferedReader reader = new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(new Path("hdfs://localhost:9000/exp2/input/stop_words.txt"))));
        	String line;
            while ((line = reader.readLine()) != null) {//按行处理
            	StringTokenizer itr=new StringTokenizer(line);
        		while(itr.hasMoreTokens()){//遍历词
        			stop_words.add(itr.nextToken());//存入vector
        		}
            }
            reader.close();
        }
        
        /**
         * map():
         * 对输入的Text切分为多个word
         * 输入：key:当前行偏移位置, value:当前行内容
         * 输出：key:word#filename, value:1
         */
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();//.toLowerCase();//获取文件名，转换为小写
            String line = value.toString().toLowerCase();//将行内容全部转为小写字母
            //只保留数字和字母
            String new_line="";
            for(int i = 0; i < line.length(); i ++) {
            	if((line.charAt(i)>=48 && line.charAt(i)<=57) || (line.charAt(i)>=97 && line.charAt(i)<=122)) {
            		new_line += line.charAt(i);
            	} else {
            		new_line +=" ";//其他字符保存为空格
            	}
            }
            //line = new_line; 
            line = new_line.trim();//去掉开头和结尾的空格
            StringTokenizer strToken=new StringTokenizer(line);//按照空格拆分
            while(strToken.hasMoreTokens()){
            	String str = strToken.nextToken();
            	if(!stop_words.contains(str)) {//不是停词则输出key-value对
            		context.write(new Text(str+"#"+fileName), new IntWritable(1));
            	}
            }
        }
	}
 
    public static class Combine extends Reducer<Text, IntWritable, Text, IntWritable> {
        /**
         * 将Map输出的中间结果相同key部分的value累加，减少向Reduce节点传输的数据量
         * 输出：key:word#filename, value:累加和
         */
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum ++;
            }
            context.write(key, new IntWritable(sum));
        }
    }
 
    public static class Partition extends HashPartitioner<Text, IntWritable> {
    //基于哈希值的分片方法
        /**
         * 为了将同一个word的键值对发送到同一个Reduce节点，对key进行临时处理
         * 将原key的(word, filename)临时拆开，使Partitioner只按照word值进行选择Reduce节点
         */
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        //第三个参数numPartitions表示每个Mapper的分片数，也就是Reducer的个数
            String term = key.toString().split("#")[0];//获取word#filename中的word
            return super.getPartition(new Text(term), value, numReduceTasks);//按照word分配reduce节点
            /*调用HashPartitioner中的分片方法
            public int getPartition(K2 key, V2 value, int numReduceTasks) {
    			return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
			  }*/           
        }
    }
 
    public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
        private String lastfile = null;//存储上一个filename
        private String lastword = null;//存储上一个word
        private String str = "";//存储要输出的value内容
        private int count = 0;
        private int totalcount = 0;
        //private StringBuilder out = new StringBuilder();//临时存储输出的value部分
        /**
         * 利用每个Reducer接收到的键值对中，word是排好序的
         * 将word#filename拆分开，将filename与累加和拼到一起，存在str中
         * 每次比较当前的word和上一次的word是否相同，若相同则将filename和累加和附加到str中
         * 否则输出：key:word，value:str
         * 并将新的word作为key继续
         * 输入：key:word#filename, value:[NUM,NUM,...]
         * 输出：key:word, value:filename:NUM;filename:NUM;...
         */
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String[] tokens = key.toString().split("#");//将word和filename存在tokens数组中
            if(lastword == null) {
            	lastword = tokens[0];
            }
            if(lastfile == null) {
            	lastfile = tokens[1];
            }
            if (!tokens[0].equals(lastword)) {//此次word与上次不一样，则将上次的word进行处理并输出
                str += "<"+lastfile+","+count+">;<total,"+totalcount+">.";
                context.write(new Text(lastword), new Text(str));//value部分拼接后输出
                lastword = tokens[0];//更新word
                lastfile = tokens[1];//更新filename
                count = 0;
                str="";
                for (IntWritable val : values) {//累加相同word和filename中出现次数
                	count += val.get();//转为int
                }
                totalcount = count;
                return;
            }
            
            if(!tokens[1].equals(lastfile)) {//新的文档
            	str += "<"+lastfile+","+count+">;";
            	lastfile = tokens[1];//更新文档名
            	count = 0;//重设count值
            	for (IntWritable value : values){//计数
            		count += value.get();//转为int
                }
            	totalcount += count;
            	return;
            }
            
            //其他情况，只计算总数即可
            for (IntWritable val : values) {
            	count += val.get();
            	totalcount += val.get();
            }
        }
 
        /**
         * 上述reduce()只会在遇到新word时，处理并输出前一个word，故对于最后一个word还需要额外的处理
         * 重载cleanup()，处理最后一个word并输出
         */
        public void cleanup(Context context) throws IOException, InterruptedException {
            str += "<"+lastfile+","+count+">;<total,"+totalcount+">.";
            context.write(new Text(lastword), new Text(str));
            
            super.cleanup(context);
        }
    }
	
	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
        String[] otherArgs=new String[]{"hdfs://localhost:9000/exp2/input","hdfs://localhost:9000/exp2/output"}; //直接设置输入参数 设置程序运行参数
        
        Job job = Job.getInstance(conf, "InvertedIndex");//设置环境参数
        job.setJarByClass(InvertedIndex.class);//设置整个程序的类名
        job.setMapperClass(Map.class);//设置Mapper类
        job.setCombinerClass(Combine.class);//设置combiner类
        job.setPartitionerClass(Partition.class);//设置Partitioner类
        job.setReducerClass(Reduce.class);//设置reducer类
        job.setOutputKeyClass(Text.class);//设置Mapper输出key类型
        job.setOutputValueClass(IntWritable.class);//设置Mapper输出value类型
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);//参数true表示检查并打印 Job 和 Task 的运行状况
	}
	
}