package com.waitingforcode;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.jackson.AsJsons;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.Iterator;

public class BeamExample {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setJobName("Apache Spark vs Apache Beam API");
        Pipeline pipeline = Pipeline.create(options);
        PCollection<Integer> inputNumbers = pipeline.apply(Create.of(
                Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));

        PCollection<Integer> numbersGreaterThan3 = inputNumbers.apply("is_greater_than_3", Filter.greaterThan(3));

        PCollection<KV<Boolean, Integer>> evenOddGroups = numbersGreaterThan3.apply("add_even_odd_flag",
                WithKeys.of(new SerializableFunction<Integer, Boolean>() {
                    @Override
                    public Boolean apply(Integer input) {
                        return input % 2 == 0;
                    }
                })
        );

        PCollection<KV<Boolean, Iterable<Integer>>> evenOddNumbersGroups = evenOddGroups
                .apply("group_by_even_or_odd", GroupByKey.create());

        PCollection<String> evenOddsWithSums = evenOddNumbersGroups.apply("sum_numbers",
                MapElements.via(new SimpleFunction<KV<Boolean, Iterable<Integer>>, LabelWithSum>() {
                    @Override
                    public LabelWithSum apply(KV<Boolean, Iterable<Integer>> input) {
                        String label = "even";
                        if (!input.getKey()) {
                            label = "odd";
                        }
                        int sum = 0;
                        Iterator<Integer> numbersIterator = input.getValue().iterator();
                        while (numbersIterator.hasNext()) {
                            sum += numbersIterator.next();
                        }
                        return new LabelWithSum(label, sum);
                    }
                }))
                .apply("convert_to_json", AsJsons.of(LabelWithSum.class));

        evenOddsWithSums.apply("write_as_json", TextIO.write().withSuffix(".json")
                .withNumShards(2)
                .to("/tmp/spark-vs-beam/beam"));

        pipeline.run().waitUntilFinish();
    }

}
