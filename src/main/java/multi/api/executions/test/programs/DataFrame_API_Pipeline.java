package multi.api.executions.test.programs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataFrame_API_Pipeline {

	public static void main(String[] args){
		
		SparkSession spk = SparkSession
				.builder()
				.master("local[2]")
				.appName("DataFrame based")
				.getOrCreate();
		
		Dataset<Row> input = spk.read().json("/home/edureka/Desktop/myapp/src/main/resources/zips.json");
		//df.show();
		input.createOrReplaceTempView("cities");
		
		Dataset<Row> five_states = spk.sql("select * from cities where state in ('MA','CA','NJ','IL','VA')");
		five_states.createOrReplaceTempView("five_states");
		//five_states.show();
		
		Dataset<Row> state_maxpop = spk.sql("select state,max(pop) as toppop from five_states group by state");
		//state_maxpop.show();
		state_maxpop.createOrReplaceTempView("state_maxpop");
		
		Dataset<Row> analysis = spk.sql("select c.state,c.loc[0] as l1,c.loc[1] as l2 from state_maxpop mp INNER JOIN cities c on mp.state=c.state where mp.toppop = c.pop ");
		//analysis.show();
		analysis.createOrReplaceTempView("analysis");
		
		Dataset<Row> result = spk.sql("select state, ( 3956 * acos( cos( radians(42.290763) ) * cos( radians( l2 ) ) * cos( radians(l1) - radians(-96.9486111)) + sin(radians(32.8138889)) * sin( radians(l2)))) AS distance  from analysis");
		//my location in Irving, TX [32.8138889,-96.9486111]
	    result.show();
		
		spk.stop();
		
	}
}
