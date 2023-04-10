
package com.stateFlinkProject;
// com.stateFlinkProject.StreamingJob
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.stateFlinkProject.streams.StreamCreator;
import com.stateFlinkProject.transformations.FastestVehicleMapper;
import com.stateFlinkProject.util.VehicleInstantData;

public class StreamingJob {

	public static void main(String[] args) throws Exception {
		String txtFilePath = "data.txt";
		String directory = "C:/temp";
		// Reading only files in the directory
		try {
		  List<File> files = Files.list(Paths.get(directory))
		    .map(Path::toFile)
		    .filter(File::isFile)
		    .collect(Collectors.toList());

		  files.forEach(System.out::println);
		} catch (IOException e) {
		  e.printStackTrace();
		}
		

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		StreamCreator streamCreator = new StreamCreator();
		
		try {
			DataStream<String> dataSource = streamCreator.getDataSourceStream(env, txtFilePath);
			
			DataStream<VehicleInstantData> vehicleInstantDataDataStream = streamCreator.mapDataSourceStreamToObject(dataSource);
	
			KeyedStream<VehicleInstantData, Tuple> keyByVehicleType = streamCreator.keyByVehicleType(vehicleInstantDataDataStream);
	
			SingleOutputStreamOperator<VehicleInstantData> fastestVehicles = streamCreator.findFastestVehicleForEachType(keyByVehicleType);
	
			fastestVehicles.print().name("PrintResult for Streamin Data Fastest car end. ");
			
		} catch(Exception e){
			System.out.println("Exception in Main streamCreator -> " + e);
			
		}

		// execute program
		env.execute("Flink Streaming Java API Skeleton is being executed ....");
	}
	
	/*
	 * public void getFiles() { ClassLoader cl = this.getClass().getClassLoader();
	 * ResourcePatternResolver resolver = new
	 * PathMatchingResourcePatternResolver(cl); Resource[] resources =
	 * resolver.getResources("images/*.png"); for (Resource r: resources){
	 * System.out.println("getFiles -> " +r.getFilename()); // From your example //
	 * ImageIO.read(cl.getResource("images/" + r.getFilename())); } }
	 */
}
