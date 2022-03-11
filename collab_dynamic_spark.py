# "My Drive/Colab Notebooks/colab_spark_setup.ipynb"
import os, sys

spark_version = '3.2.1'
hadoop_version = '3.2'

def colab_install_spark():
    #!apt-get install openjdk-8-jdk-headless -qq > /dev/null
    if os.path.exists('spark-'+spark_version+'-bin-hadoop'+hadoop_version):
        print('spark already installed')
    else:
        tgz_file = 'spark-'+spark_version+'-bin-hadoop'+hadoop_version+'.tgz'
        if os.path.exists(tgz_file):
            print('skip spark download')
        else:
            print('downloading spark '+ spark_version)
            downlowd_url = 'https://downloads.apache.org/spark/spark-'+spark_version+'/'+tgz_file
            !wget -q {downlowd_url}
        print('tar xf spark')
        !tar xf {tgz_file}
    #os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
    #os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
    os.environ["SPARK_HOME"] = "/content/spark-"+spark_version+"-bin-hadoop"+hadoop_version
    if 'findspark' in sys.modules.keys():
        print('findspark already installed')    
    else:
        print('installing findspark')
        !pip install -q findspark

def start_spark():
    import findspark
    findspark.init()
    from pyspark.sql import SparkSession
    # You can create multiple SparkSession objects
    # but only one SparkContext per JVM.
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    print(spark)
    return spark

IN_COLAB = False
if 'COLAB_GPU' in os.environ:
   print("I'm running on Colab")
   IN_COLAB = True

if IN_COLAB:
    print('installing spark')
    colab_install_spark()
    print('starting spark')
    spark = start_spark()
    print("Spark version: ", spark.sparkContext.version)

#spark

# simple rdd test:
#spark.sparkContext.parallelize([1,2,3,4,5,6,7,8,9,10]).count()

# in your new notebook:
'''
from google.colab import drive
drive.mount('/content/gdrive')
%run /content/gdrive/My\ Drive/Colab\ Notebooks/colab_spark_setup.ipynb
'''
