package com.sxt.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by ASUS on 2018/12/5.
 */
public class Operator_combinerByKey {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("countByKey");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaPairRDD <String, Integer> parallelizePairs = jsc.parallelizePairs(Arrays.asList(
                new Tuple2<>("zhangsan", 10),
                new Tuple2 <>("lisi", 11),
                new Tuple2 <>("zhangsan", 12),
                new Tuple2 <>("zhangsan", 13),
                new Tuple2 <>("lisi", 14),
                new Tuple2 <>("wangwu", 15),
                new Tuple2 <>("lisi", 16)
        ), 2);
        parallelizePairs.combineByKey(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return null;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return null;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return null;
            }
        }).foreach(new VoidFunction <Tuple2 <String, Integer>>() {
            @Override
            public void call(Tuple2 <String, Integer> tuple2) throws Exception {
                System.out.println(tuple2);
            }
        });
    }
}
