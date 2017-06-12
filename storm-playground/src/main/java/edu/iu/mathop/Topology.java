package edu.iu.mathop;

import edu.iu.mathop.bolt.BoltAdd;
import edu.iu.mathop.bolt.BoltMultiply;
import edu.iu.mathop.bolt.BoltSubtract;
import edu.iu.mathop.spout.SpoutAdd;
import edu.iu.mathop.spout.SpoutMultiply;
import edu.iu.mathop.spout.SpoutSubtract;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by goshenoy on 6/11/17.
 */
public class Topology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spoutadd", new SpoutAdd());
        builder.setBolt("add", new BoltAdd()).shuffleGrouping("spoutadd");

        builder.setSpout("spoutsubtract", new SpoutSubtract());
        builder.setBolt("subtract", new BoltSubtract()).shuffleGrouping("spoutsubtract");

        builder.setSpout("spoutmultiply", new SpoutMultiply());
        builder.setBolt("multiply", new BoltMultiply()).shuffleGrouping("spoutmultiply");

        Config conf = new Config();
        conf.setDebug(false);

        conf.setMaxTaskParallelism(10);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("BoltAdd", conf, builder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();
    }
}
