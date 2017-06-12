package edu.iu.mathadd;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Created by goshenoy on 6/9/17.
 */
public class BoltAdd extends BaseBasicBolt {

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        System.out.println("Tuple: " + tuple);
        String example = tuple.getString(0);
        int a = 0;
        int b = 0;

        for(int i = 0; i < 3; i++)
        {

            if(i == 0) {
                if(tryParse(Character.toString(example.charAt(i))) == null)
                    a = 2;
                else
                    a = Integer.parseInt(Character.toString(example.charAt(i)));
            }
            else if(i == 2)
                b = Integer.parseInt(Character.toString(example.charAt(i)));
        }

        int sum = a + b;
        System.out.println("The added answer is: " + (sum));
        collector.emit(new Values(a));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("adder"));
    }

    private static Integer tryParse(String text) {
        try {
            return Integer.parseInt(text);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
