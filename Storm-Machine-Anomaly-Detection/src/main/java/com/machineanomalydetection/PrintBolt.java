package com.machineanomalydetection;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class PrintBolt extends BaseRichBolt {

    private OutputCollector collector;

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

        boolean vibrationEvent
                = input.getBooleanByField("vibration_event");

        boolean temperatureEvent
                = input.getBooleanByField("temperature_event");

        System.out.printf(
                "[PRINT] machine_id=%d, vibration_event=%b, temperature_event=%b%n",
                machineId,
                vibrationEvent,
                temperatureEvent
        );

        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No output fields: this is the final bolt.
    }
}
