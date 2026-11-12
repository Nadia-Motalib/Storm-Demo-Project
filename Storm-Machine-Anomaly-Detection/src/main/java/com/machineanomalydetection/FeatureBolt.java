package com.machineanomalydetection;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class FeatureBolt extends BaseRichBolt {

    private OutputCollector collector;

    private final Map<Integer, Double> previousTemperatures
            = new HashMap<>();

    @Override
    public void prepare(
            Map<String, Object> conf,
            TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        int machineId = input.getIntegerByField("machine_id");

        double vibration = input.getDoubleByField("vibration");
        double temperature = input.getDoubleByField("temperature");

        // Example feature: squared vibration magnitude.
        double vibrationScore = vibration * vibration;

        Double previousTemperature
                = previousTemperatures.get(machineId);

        // The first reading has no previous temperature.
        double temperatureDiff
                = previousTemperature == null
                        ? 0.0
                        : temperature - previousTemperature;

        previousTemperatures.put(machineId, temperature);

        collector.emit(input, new Values(
                machineId,
                vibrationScore,
                temperatureDiff
        ));

        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                "machine_id",
                "vibration_score",
                "temperature_diff"
        ));
    }
}
