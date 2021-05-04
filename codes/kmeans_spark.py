from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Kmeans-Spark").getOrCreate()


from pyspark.ml.linalg import Vectors
from pyspark.ml.clustering import KMeans
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import ClusteringEvaluator
import sys

inputPath = sys.argv[1]

puntos=spark.read.csv(inputPath,inferSchema=True)
assembler = VectorAssembler(inputCols=puntos.columns, outputCol="features")
df=assembler.transform(puntos).select("features")
kmeans = KMeans(k=3, seed=1,maxIter=10)
kmModel = kmeans.fit(df)
df_pred = kmModel.transform(df)
evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(df_pred)
print("Silhouette with squared euclidean distance = " + str(silhouette))
l_clusters = kmModel.clusterCenters()
# Let's convert the list of centers to a dict, each center is a list of float
d_clusters = {int(i):[float(l_clusters[i][j]) for j in range(len(l_clusters[i]))] 
              for i in range(len(l_clusters))}

# Let's create a dataframe containing the centers and their coordinates
df_centers = spark.sparkContext.parallelize([(k,)+(v,) for k,v in 
d_clusters.items()]).toDF(['prediction','center'])

df_pred = df_pred.withColumn('prediction',F.col('prediction').cast(IntegerType()))
df_pred = df_pred.join(df_centers,on='prediction',how='left')
df_pred.show()
