package com.machineanomalydetection;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class ExpandBolt extends BaseRichBolt {

    private OutputCollector collector;

    private static final double VIBRATION_THRESHOLD = 64.0;
    private static final double TEMPERATURE_DIFF_THRESHOLD = 15.0;

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

        double vibrationScore
                = input.getDoubleByField("vibration_score");

        double temperatureDiff
                = input.getDoubleByField("temperature_diff");

        boolean vibrationEvent
                = vibrationScore > VIBRATION_THRESHOLD;

        boolean temperatureEvent
                = Math.abs(temperatureDiff) > TEMPERATURE_DIFF_THRESHOLD;
 
        collector.emit(input, new Values(
                machineId,
                vibrationEvent,
                temperatureEvent
        ));

        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                "machine_id",
                "vibration_event",
                "temperature_event"
        ));
    }
}
