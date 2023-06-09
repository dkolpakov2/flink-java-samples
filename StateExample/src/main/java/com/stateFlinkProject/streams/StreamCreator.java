package com.stateFlinkProject.streams;

import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

import com.stateFlinkProject.util.CountDataTemp;
import com.stateFlinkProject.transformations.FastestVehicleMapper;
import com.stateFlinkProject.transformations.MapToObjectTransformation;
import com.stateFlinkProject.util.VehicleInstantData;


public class StreamCreator
{
    public DataStream<String> getDataSourceStream(StreamExecutionEnvironment env, String filePath)
    {
	
    	//todo add s3 file
        //return env.readTextFile(filePath).name("IBBDataSource");
    	DataStream<String> source = env.fromElements(CountDataTemp.CARS).name("CountAutoDataSource");
        return source;
    }

    public DataStream<String> getContinuouslyDataSourceStream(StreamExecutionEnvironment env, String filePath)
    {
        TextInputFormat format = new TextInputFormat(new Path(filePath));
        format.setFilesFilter(FilePathFilter.createDefaultFilter());
        TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
        format.setCharsetName("UTF-8");
        return env.readFile(format, filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 10, typeInfo);
    }

    public DataStream<VehicleInstantData> mapDataSourceStreamToObject(DataStream<String> dataSourceStream)
    {
        return dataSourceStream.map(new MapToObjectTransformation()).name("MapTxtToObject");
    }

    public KeyedStream<VehicleInstantData, Tuple> keyByVehicleType(DataStream<VehicleInstantData> vehicleStream)
    {
        return vehicleStream
                .keyBy("vehicle_type");
    }

    public SingleOutputStreamOperator<VehicleInstantData> findFastestVehicleForEachType(KeyedStream<VehicleInstantData, Tuple> vehicleKeyedStreamByVehicleType)
    {
        return vehicleKeyedStreamByVehicleType.map(new FastestVehicleMapper()).name("FindFastestVehicle");
    }
}
