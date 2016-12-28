package multi.api.executions.test.programs;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

import multi.api.executions.test.helper.City;
import multi.api.executions.test.helper.Util;

public class RDD_API_Pipeline {

	public static void main(String[] args){
		
		SparkConf conf = new SparkConf();
		conf.setMaster("local[2]");
		conf.setAppName("RDD-based");
		
				
		JavaSparkContext jsc = new JavaSparkContext(new SparkContext(conf));
		
		JavaRDD<City> inputRDD = jsc.parallelize(Util.loaddata("/home/edureka/Desktop/myapp/src/main/resources/zips.json"));
		
		JavaRDD<City> five_states = inputRDD.filter(new Function<City,Boolean>(){
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Boolean call(City c){
				
				return (c.getState().equals("MA")||c.getState().equals("CA")||c.getState().equals("NJ")||c.getState().equals("IL")||c.getState().equals("VA"));
			}
			
		});
		
		JavaPairRDD<String,Long> state_maxpop = five_states.mapToPair(new PairFunction<City,String,Long>(){
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<String,Long> call(City c){
				return new Tuple2<String,Long>(c.getState(),c.getPop());
			}
		}).reduceByKey(new Function2<Long,Long,Long>(){
		
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Long call(Long l1, Long l2){
				
				return Math.max(l1,l2);
				
			}
			
		});
		
				
		JavaPairRDD<String,List<Double>> analysis = state_maxpop.join(inputRDD.mapToPair(new PairFunction<City,String,City>(){
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<String,City> call(City c){
				return new Tuple2<String,City>(c.getState(),c);
			}
			
		})).filter(new Function<Tuple2<String,Tuple2<Long,City>>,Boolean>(){
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Boolean call(Tuple2<String,Tuple2<Long,City>> tupl){
				
				return tupl._2._2.getPop().equals(tupl._2._1);
			}
		}).mapValues(new Function<Tuple2<Long,City>,List<Double>>(){
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public List<Double> call(Tuple2<Long,City> tup){
				return tup._2.getLoc();
			}
		});
		
				
		JavaPairRDD<String,Double> result = analysis.mapValues(new Function<List<Double>,Double>(){
			
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public	Double call(List<Double> lst){
				
				return distance(lst.get(1),lst.get(0),0,0);
				
			}});
		
		result.foreach(new VoidFunction<Tuple2<String,Double>>(){
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public void call(Tuple2<String,Double> tup){
				//System.out.println(city.getCity()+city.getLoc().get(0)+city.getPop()+city.getState()+city.get_id());
				System.out.println(tup._1+","+tup._2);
			}
		});
		
		jsc.stop();
		jsc.close();
	}
	   private static double distance(Double lat2, double lon2, double el1, double el2){

	        final int R = 6371; // Radius of the earth
	        double myLat = 32.8138889;
			double myLon = -96.9486111;
	        //my location in Irving, TX [32.8138889,-96.9486111]

	        Double latDistance = Math.toRadians(lat2 - myLat);
	        Double lonDistance = Math.toRadians(lon2 - myLon);
	        Double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)+ Math.cos(Math.toRadians(myLat)) * Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
	        Double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
	        double distance = R * c; // convert to meters
            double height = el1 - el2;
            distance = Math.pow(distance, 2) + Math.pow(height, 2);//in kilometers

	        return Math.sqrt(distance);
	      }
     }
