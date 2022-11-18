import os
import socket
import random
import hashlib

import jellyfish
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark import SparkConf

from settings import SPARK_DISTRIBUTED_FILE_SYSTEM
from connector import HDFSConnector as HDFS

EXTRACT_DIRECTORY = SPARK_DISTRIBUTED_FILE_SYSTEM + "input/"
LOAD_DIRECTORY = SPARK_DISTRIBUTED_FILE_SYSTEM + f'pretransformed_data'
MATCHED_DIRECTORY = SPARK_DISTRIBUTED_FILE_SYSTEM + f'matched_data'
JOINED_DIRECTORY = SPARK_DISTRIBUTED_FILE_SYSTEM + f'joined_data'


class ThesisSparkClassETLModel:
    """
        This is the ETL class model that will be used in backend for our application.
        The ETL pipeline is responsible for the extraction, transformation and loading
        the data.
    """

    def __init__(self,
                 project_name: str,
                 hdfs: HDFS,
                 filename: str,
                 matching_field: str = '',
                 columns: list = None,
                 noise: int = None):
        self.project_name = project_name
        self.hdfs_obj = hdfs
        self.matching_field = matching_field
        self.filename = filename
        self.noise = noise
        self.columns = columns
        self.dataframe = None
        self.df_with_noise = None

        spark_driver_host = socket.gethostname()
        self.spark_conf = SparkConf() \
            .setAll([
            ('spark.master', 'spark://spark-master:7077'),
            ('spark.driver.bindAddress', '0.0.0.0'),
            ('spark.driver.host', spark_driver_host),
            ('spark.app.name', self.project_name),
            ('spark.submit.deployMode', 'client'),
            ('spark.ui.showConsoleProgress', 'true'),
            ('spark.eventLog.enabled', 'false'),
            ('spark.logConf', 'false'),
            ('spark.cores.max', "4"),
            ("spark.executor.memory", "1g"),
            ('spark.driver.memory', '15g'),
            ('spark.submit.pyFiles', '/src/app/udf_files.zip')
        ])

        self.spark = SparkSession.builder\
            .config(conf=self.spark_conf) \
            .enableHiveSupport() \
            .getOrCreate()
        self.spark.sparkContext.accumulator(0)

    @staticmethod
    @udf(StringType())
    def jelly(data):
        return jellyfish.soundex(data)

    @staticmethod
    @udf(StringType())
    def hash_sha256(data):
        return hashlib.sha256(data.encode()).hexdigest()

    def create_alp(self) -> str:
        return str(chr(random.randrange(65, 90))) + str(chr(random.randrange(48, 54))) + str(
            chr(random.randrange(48, 54))) + str(chr(random.randrange(48, 54)))

    def extract_data(self):
        self.dataframe = self.spark.read.csv(EXTRACT_DIRECTORY + self.filename, header=True)

    def transform_data(self):
        self.dataframe = self.dataframe.na.drop('any')
        columns = self.dataframe.columns
        size = int(self.dataframe.count() * (self.noise / 100))
        data = [{colum: (self.create_alp() if not colum == self.matching_field else 'Fake Index') for colum in columns}
                for _ in range(0, size)]

        # create noise
        df_with_noise = self.spark.createDataFrame(data, columns)

        # add noise
        self.dataframe = self.dataframe.union(df_with_noise)

        # apply Soundex + SHA256
        self.dataframe = self.dataframe.select([self.hash_sha256(self.jelly(col(column))).alias(column)
                                                if not column == self.matching_field else col(column).alias(column)
                                                for column in columns])

        # sort by a random column
        self.dataframe = self.dataframe.sort(random.choice(columns))

    def load_data(self):
        """
        Save the files in a specific directory in the HDFS
        Returns:

        """
        # self.dataframe.coalesce(1).write.format('com.databricks.spark.csv'). \
        #     mode('overwrite'). \
        #     save(LOAD_DIRECTORY, header='true')
        self.dataframe.toPandas().to_csv(os.path.join(LOAD_DIRECTORY, 'transformed_data.csv'), index=False)

    def start_etl(self):
        try:
            self.extract_data()
            self.transform_data()
            self.load_data()
            self.spark.stop()
        except Exception as e:
            return e


class ThesisSparkClassCheckFake(ThesisSparkClassETLModel):

    def __init__(self, project_name: str, hdfs: HDFS, filename: str, joined_data_filename: str):
        super().__init__(project_name, hdfs, filename)
        self.joined_data_filename = joined_data_filename
        self.dataframe_joined_data = None
        self.matched_data = None
        self.df_columns = []

    def set_matching_field(self):
        import re

        self.df_columns = [name for name, value in self.dataframe.take(1)[0].asDict().items()]
        if "_c0" in self.df_columns:
            self.df_columns.remove("_c0")

        matching_field = [name for name, value in self.dataframe.take(1)[0].asDict().items() if not re.match("[0-9a-f]{64}", value)]
        matching_field.remove("_c0")
        if "_c0" in self.df_columns:
            self.df_columns.remove("_c0")

        self.matching_field = str(matching_field[0])

    def get_matching_field(self):
        return self.matching_field

    def extract_data(self):
        self.dataframe = self.spark.read.csv(os.path.join(LOAD_DIRECTORY, self.filename), header=True)
        path_ur = os.path.join(JOINED_DIRECTORY, self.joined_data_filename + '/results.csv')
        self.dataframe_joined_data = self.spark.read.csv(path=path_ur, header=True)

    def transform_data(self):
        self.set_matching_field()
        self.dataframe_joined_data = self.dataframe_joined_data.withColumnRenamed(self.matching_field, "MatchingField")
        self.dataframe = self.dataframe.withColumnRenamed(self.matching_field, "MatchingField")
        self.dataframe = self.dataframe.na.drop('any')

        self.matched_data = self.dataframe.join(other=self.dataframe_joined_data, on=["MatchingField"], how='left')\
            .select(self.dataframe["*"])\
            .where("MatchingField!='Fake Index'")

        self.dataframe = self.dataframe.withColumnRenamed("MatchingField", self.matching_field)
        self.matched_data = self.matched_data.drop(*(colms for colms in self.matched_data.columns if colms not in self.df_columns))


    def load_data(self):
        # self.matched_data.coalesce(1).write.format('com.databricks.spark.csv'). \
        #     mode('overwrite'). \
        #     save(MATCHED_DIRECTORY, header='true')
        directory = os.path.join(SPARK_DISTRIBUTED_FILE_SYSTEM, 'matched_data', f'{self.joined_data_filename.lower()}')
        if not os.path.exists(directory):
            os.mkdir(directory)
        path_ur = os.path.join(directory, 'results.csv')
        self.matched_data.toPandas().to_csv(path_ur, index=False)
