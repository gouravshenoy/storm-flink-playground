package edu.iu.mathadd;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Created by goshenoy on 6/9/17.
 */
public class SpoutInput extends BaseRichSpout {

    SpoutOutputCollector _collector;
    Random _rand;
    int counter = 0;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
        _rand = new Random();
    }

    public void nextTuple() {
        Utils.sleep(100);
        System.out.println("Called!!");
        String [] example = {"2 4","3 6","5 7"};
        if(counter > example.length-1) return;
        String character = example[counter];

        System.out.println("Emitting: "+ character);
        _collector.emit(new Values(character));
        counter++;
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}
