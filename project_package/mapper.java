package sentiment_analysis_package;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;


public class mapper extends Mapper<LongWritable,Text,Text,FloatWritable>
{
    public int String_substr(String mainstr,String substr)
    {
        int main=mainstr.length();
        int sub=substr.length();

        int i,j,con=0;
        int res=0;

        for(i=0;i<=main-sub;i++)
        {
            for (j = 0; j <= sub - 1; j++)
            {
                if (mainstr.charAt(i + j) == substr.charAt(j))
                    con++;
                if (con == sub)
                {
                    res=1;
                    break;
                }


            }
            if(con==sub)
                break;
            con=0;

        }

        return res;
    }
    public void map(LongWritable inkey,Text value,Context context) throws IOException, InterruptedException
    {

        ArrayList<String> positive=new ArrayList<String>();

        Path pt=new Path("hdfs://localhost:8020/decider/positive.txt");
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line2;
        line2=br.readLine();
        while (line2 != null){
            String []x = line2.split("\t");
            for(String each : x){
                if(!"".equals(each)){
                    positive.add(line2);
                }
            }
            line2=br.readLine();
        }

        ArrayList<String> negative=new ArrayList<String>();

        Path pt1=new Path("hdfs://localhost:8020/decider/negative.txt");//Location of file in HDFS
        FileSystem fs1 = FileSystem.get(new Configuration());
        BufferedReader br1=new BufferedReader(new InputStreamReader(fs1.open(pt1)));
        String line1;
        line1=br1.readLine();
        while (line1 != null){
            String []x = line1.split("\t");
            for(String each : x){
                if(!"".equals(each)){
                    negative.add(line1);
                }
            }
            line1=br1.readLine();
        }

        String line=value.toString();
        String lineParts[] = line.split("\"");

        int i=0;
        int res1=0,res2=0;
        
        for(String singlestring:lineParts)
        {
            if(singlestring.equals("text")==true)
            {
                res1=i;
            }
            
            if(singlestring.equals("contributors")==true)
            {
            	res2=i;
            	break;
            }
            i++;
        }
        
        int j=res2-res1-3;
        String text=" ";
        
        for(i=0;i<=j-1;i++)
        {
        	text=text+lineParts[res1+2+i];
        }
        

        
        String result1="positive";
        
        float positive_and_negative_no=0;
        float positive_no=0;
        
        for(String comp:negative)
        {
            if(String_substr(text,comp)==1)
            {
            	positive_and_negative_no++;
            }
        }

        for(String comp:positive)
        {
            if(String_substr(text,comp)==1)
            {
            	positive_no++;
            	positive_and_negative_no++;
            }

        }
        
        float result2=(positive_no/positive_and_negative_no);
        
        Text OutKey = new Text(result1);
        //IntWritable One = new IntWritable(1);
        FloatWritable One=new FloatWritable(result2);
        context.write(OutKey, One);

    }
}

