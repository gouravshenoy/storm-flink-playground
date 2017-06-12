package edu.iu.mathop.spout;

import edu.iu.mathop.Data;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * Created by goshenoy on 6/11/17.
 */
public class SpoutSubtract extends BaseRichSpout {
    SpoutOutputCollector spoutOutputCollector;
    int counter;


    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    public void nextTuple() {
        Utils.sleep(1000);
        if (++this.counter > 1) {
            System.out.println("Substract Spout > 1, returning!");
            return;
        }
        this.spoutOutputCollector.emit(new Values(
                new Data(8, 4)
        ));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("mathdata"));
    }
}
