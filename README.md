# python-spark-sample
python files for data analysis

1. create repository using **your profile** -> **create repository** -> Add **Name** in the name field of the **project** -> tick **readme** file creation

python code for data processing of the data from  https://perso.telecom-paristech.fr/eagan/class/igr204/datasets 

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder
    .appName("DataProcessing") 
    .getOrCreate()

data_df = spark.read.csv(<file_path>, header=True, inferSchema=True)

filtered_df = data_df.filter(col("cylinders") == 8)
selected_columns_df = filtered_df.select("car", "HorsePower")

#save the file to output file
selected_columns_df.write.csv(<file-path>, header=True, mode="overwrite")


python object implementation:

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class DataProcessor:
    def __init__(self, input_path, output_path):
        self.input_path = input_path
        self.output_path = output_path
        self.spark = SparkSession.builder \
            .appName("DataProcessing") \
            .getOrCreate()

    def load_data(self):
        self.data_df = self.spark.read.csv(self.input_path, header=True, inferSchema=True)

    def process_data(self):
        filtered_df = data_df.filter(col("cylinders") == 8)
        selected_columns_df = filtered_df.select("car", "HorsePower")

    def save_processed_data(self):
        self.selected_columns_df.write.csv(self.output_path, header=True, mode="overwrite")


if __name__ == "__main__":
    input_path = "path/to/input.csv"
    output_path = "path/to/output.csv"

    data_processor = DataProcessor(input_path, output_path)

    data_processor.load_data()
    data_processor.process_data()
    data_processor.save_processed_data()
    data_processor.close_session()
