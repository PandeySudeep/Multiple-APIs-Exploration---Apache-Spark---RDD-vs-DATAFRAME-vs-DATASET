package multi.api.executions.test.programs;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import scala.Tuple3;

import multi.api.executions.test.helper.City;

public class DataSet_API_Pipeline {
	
	public static void main(String[] args){
		
		SparkSession spk = SparkSession
				.builder()
				.master("local[2]")
				.appName("DataSet based")
				.getOrCreate();
		
		Encoder<City> cityEncoder = Encoders.bean(City.class);
		Encoder<String> strEncoder = Encoders.STRING();
		Encoder<Tuple3<String,Double,Double>> tupEncoder2 = Encoders.tuple(Encoders.STRING(),Encoders.DOUBLE(),Encoders.DOUBLE());
		Encoder<Tuple2<String,Double>> tupEncoder3 = Encoders.tuple(Encoders.STRING(),Encoders.DOUBLE());
		
		Dataset<City> input = spk.read().json("/home/edureka/Desktop/myapp/src/main/resources/zips.json").as(cityEncoder);
		
		Dataset<City> five_states = input.filter("state IN ('MA','CA','NJ','IL','VA')");
		
		Dataset<Tuple2<String,City>> state_maxpop = five_states.groupByKey(new MapFunction<City,String>(){
		
					private static final long serialVersionUID = 1L;

					public String call(City c){
						return c.getState();
					}
			
			},strEncoder).reduceGroups(new ReduceFunction<City>(){
		
				private static final long serialVersionUID = 1L;

				public City call(City c1, City c2){
					City c=null;
					if(c1.getPop()>c2.getPop()){
						c=c1;}
					else if (c1.getPop()<c2.getPop()) {c=c2;}
			return c;	
				}
			});
				
			Dataset<Tuple3<String,Double,Double>> analysis = state_maxpop.map(new MapFunction<Tuple2<String,City>,Tuple3<String,Double,Double>>(){
		
				private static final long serialVersionUID = 1L;

				public Tuple3<String,Double,Double>call(Tuple2<String,City> tup){
				
					return new Tuple3<String,Double,Double>(tup._1,tup._2.getLoc().get(0),tup._2.getLoc().get(1));
				}
			},tupEncoder2);
			
			Dataset<Tuple2<String,Double>> results = analysis.map(new MapFunction<Tuple3<String,Double,Double>,Tuple2<String,Double>>(){
				
		    	private static final long serialVersionUID = 1L;

				public Tuple2<String,Double> call(Tuple3<String,Double,Double> tup){
					return new Tuple2<String,Double>(tup._1(), distance(tup._3(),tup._2(),0,0));
					}
				
				},tupEncoder3);
				
				
				results.show();
				
				spk.stop();
		}
	
	      	private static double distance(Double lat2, double lon2, double el1, double el2){

	      		final int R = 6371; // Radius of the earth
	      		double myLat = 32.8138889;
	      		double myLon = -96.9486111;
	      		//my location in Irving, TX [32.8138889,-96.9486111]

	      		double latDistance = Math.toRadians(lat2 - myLat);
	      		double lonDistance = Math.toRadians(lon2 - myLon);
	      		double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)+ Math.cos(Math.toRadians(myLat)) * Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
	      		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
	      		double distance = R * c; // convert to meters
	      		double height = el1 - el2;
	      		
	      		distance = Math.pow(distance, 2) + Math.pow(height, 2);//in kilometers

	      		return Math.sqrt(distance);
	      	}

	}
