package edu.iu.mathop.bolt;

import edu.iu.mathop.Data;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Created by goshenoy on 6/11/17.
 */
public class BoltSubtract extends BaseBasicBolt {
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        Data data = (Data) tuple.getValue(0);
        System.out.println("BoltSubtract | data: " + data);
        basicOutputCollector.emit(new Values(data.getNumber1() - data.getNumber2()));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("mathsubtract"));
    }
}
