package com.impetus.se.main;

import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

//import java.io.File;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class xmlparser {
	
	public static void main(String args[]) {
		SparkSession spark = SparkSession.builder().master("local").appName("xml parser").getOrCreate();
		
		
		
		 StructType schema = DataTypes.createStructType(new StructField[] {
		            DataTypes.createStructField("id",  DataTypes.StringType, true),
		            DataTypes.createStructField("active_dtmz", DataTypes.DateType, true),
		            DataTypes.createStructField("inactive_dtmz", DataTypes.DateType, true),
		            DataTypes.createStructField("utilization_type", DataTypes.StringType, true)
		    });
		
		Dataset<Row> df = spark
						.read()
						.schema(schema)
						.format("csv")
						.option("dateFormat","MM/dd/yyyy")
						.load("C:\\Users\\somant.kumar\\Documents\\idl\\test.csv");
	
		
		
		LocalDateTime currentTime = LocalDateTime.now();
		LocalDate currentDate = currentTime.toLocalDate();
		df = df.withColumn("day", functions.lit(Date.valueOf(currentDate)));
		
		
		df.printSchema();
		df.show(false);
//		Dataset<Row> data_df_active = df.filter(df.col("inactive_dtmz").cast(DataTypes.DateType).equalTo(functions.lit("3001-01-01")));

//		data_df_active.show();
//		
//		
//		LocalDateTime currentTime = LocalDateTime.now();
//		LocalDate currentDate = currentTime.toLocalDate();
//		LocalDate oneweekEarlierDate  = currentDate.minus(7,ChronoUnit.DAYS);
//		
//		Dataset<Row> data_df_inactive = df.filter(df.col("inactive_dtmz").cast(DataTypes.DateType).between(functions.lit(oneweekEarlierDate.toString()),functions.lit(currentDate.toString())));
//		
//		data_df_inactive.show();
//		WindowSpec windowOperation = Window.partitionBy("id")
//				.orderBy("active_dtmz");
//
//		
//		data_df_inactive = data_df_inactive.withColumn("rank",functions.row_number().over(windowOperation));
//		data_df_inactive = data_df_inactive.filter(functions.col("rank").equalTo(1)).drop("rank");
//		data_df_inactive.show();
//		
//		
//		Dataset<Row> data_df_active_2 = data_df_active.filter(data_df_active.col("active_dtmz").cast(DataTypes.DateType).leq(functions.lit(oneweekEarlierDate.toString())));
//		
//		data_df_active_2.show();
//		
//		
//		Dataset<Row> data_df_7_days_ago = data_df_active_2.union(data_df_inactive);
//		
//		data_df_7_days_ago.show();
		
		
				
	}

}
