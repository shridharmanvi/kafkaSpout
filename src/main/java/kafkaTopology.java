import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.*;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import storm.kafka.*;


import java.util.Map;

/**
 * Created by shridhar.manvi on 6/16/16.
 */
public class kafkaTopology {


        public static class PrinterBolt implements IRichBolt {

                public void declareOutputFields(OutputFieldsDeclarer declarer) {
                        //End bolt. No output field needed!
                }

                public Map<String, Object> getComponentConfiguration() {
                        return null;
                }


                public void execute(Tuple tuple, BasicOutputCollector collector) {
                        System.out.println("Its here........----------===================++++++++++++++++");
                        System.out.println((tuple.toString()));
                }

                public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

                }

                public void execute(Tuple tuple) {

                }

                public void cleanup() {

                }
        }


        public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
                System.out.println("Starting....");
                //String zkPort = ":2181";
                String zookeeperHost = "vapp-zookeeper-002.nl-ams.ignitionone.com:2181";
                //String nimbusHost = "http://localhost/6627:";
                //ZkHosts zkHosts = new ZkHosts(zookeeperHost);
                BrokerHosts zkHosts= new ZkHosts(zookeeperHost);

                SpoutConfig spConfig = new SpoutConfig(zkHosts, "conversions_php", "Shridhar-mySpoutID" , "storm");
                spConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

                KafkaSpout kafkaSpout = new KafkaSpout(spConfig);

                TopologyBuilder builder = new TopologyBuilder();

                builder.setSpout("eventsEmitter", kafkaSpout);
                builder.setBolt("eventBolt", new PrinterBolt());

                //builder.createTopology();

                Config config = new Config();
                config.setDebug(false);
                config.setMaxSpoutPending(1);


                LocalCluster cluster = new LocalCluster();

                try {
                        cluster.submitTopology("myTopology",config, builder.createTopology());
                        //StormSubmitter.submitTopology("myTopology",config, builder.createTopology());
                        System.out.println("Sleeping ======================");
                        Thread.sleep(10000);

                        cluster.shutdown();

                }
                catch (Exception e){
                        System.out.println("Facing issue +++++++++++++++++++++++++++++++++++++");
                }



        }







}
