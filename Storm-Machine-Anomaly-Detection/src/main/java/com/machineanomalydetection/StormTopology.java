package com.machineanomalydetection;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class StormTopology {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        // 1. Source
        builder.setSpout(
                "mgtt-source",
                new MqttSourceSpout(), 1
        );

        // 2. Filter
        builder.setBolt(
                "filter-operator",
                new FilterBolt(), 2
        ).shuffleGrouping("mgtt-source");

        // 3. Feature extraction
        builder.setBolt(
                "feature-operator",
                new FeatureBolt(), 2
        ).fieldsGrouping(
                "filter-operator",
                new Fields("machine_id")
        );

        // 4. Diagnosis
        builder.setBolt(
                "expand-operator",
                new ExpandBolt(), 2
        ).shuffleGrouping("feature-operator");

        // 5. Print
        builder.setBolt(
                "print-operator",
                new PrintBolt(), 1
        ).shuffleGrouping("expand-operator");

        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(2);

        String topologyName = "sensor-processing-topology";

        StormSubmitter.submitTopology(
                topologyName,
                config,
                builder.createTopology()
        );
        System.out.println("Topology submitted successfully.");
    }
}
