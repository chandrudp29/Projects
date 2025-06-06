from pyspark.sql import SparkSession
import sys

def main(input_path, output_path):
    spark = SparkSession.builder.appName("WordCount").getOrCreate()
    
    text_df = spark.read.text(input_path)
    words = text_df.selectExpr("explode(split(value, ' ')) as word")
    
    word_counts = words.groupBy("word").count()
    
    word_counts.write.mode("overwrite").csv(output_path)
    
    spark.stop()

if __name__ == "__main__":
    input_path = sys.argv[1]  # e.g. 'data/input.txt'
    output_path = sys.argv[2] # e.g. 'data/output'
    main(input_path, output_path) # Run the main function with input and output paths
# Example usage: spark-submit wordcount.py data/input.txt data/output

