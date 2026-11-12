package com.machineanomalydetection;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class StormTopology {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        // 1. Source: generate random sensor data.
        builder.setSpout(
                "mgtt-source",
                new MqttSourceSpout(),
                1
        );

        // 2. Filter: validate readings.
        builder.setBolt(
                "filter-operator",
                new FilterBolt(),
                2
        ).shuffleGrouping("mgtt-source");

        // 3. Feature extraction: group by machine so that
        // readings for each machine reach the same task.
        builder.setBolt(
                "feature-operator",
                new FeatureBolt(),
                2
        ).fieldsGrouping(
                "filter-operator",
                new Fields("machine_id")
        );

        // 4. Diagnosis: classify the extracted features.
        builder.setBolt(
                "expand-operator",
                new ExpandBolt(),
                2
        ).shuffleGrouping("feature-operator");

        // 5. Print: display the final diagnosis.
        builder.setBolt(
                "print-operator",
                new PrintBolt(),
                1
        ).shuffleGrouping("expand-operator");

        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(2);

        String topologyName = "sensor-processing-topology";

        LocalCluster cluster = new LocalCluster();

        try {
            cluster.submitTopology(
                    topologyName,
                    config,
                    builder.createTopology()
            );

            System.out.println("Topology started.");

            Utils.sleep(30000);

        } finally {
            cluster.killTopology(topologyName);
            cluster.shutdown();
        }

        System.out.println("Topology stopped.");
    }
}
