# definately needs improvement

# https://downloads.apache.org/spark/
# when they updated spark-3.0.1, they deleted spark-3.0.0, supprise!
# so need to use a variable to easily change url, file names and envion
# this needs work

import os, sys

spark_versioin = '3.0.1'

def colab_install_spark():
    #!apt-get install openjdk-8-jdk-headless -qq > /dev/null
    if os.path.exists('spark-'+spark_versioin+'-bin-hadoop3.2'):
        print('spark already installed')
    else:
        if os.path.exists('spark-'+spark_versioin+'-bin-hadoop3.2.tgz'):
            print('skip spark download')
        else:
            print('downloading spark '+ '+spark_versioin+')
            !wget -q https://downloads.apache.org/spark/spark-3.0.1/spark-3.0.1-bin-hadoop3.2.tgz
        print('tar xf spark')
        !tar xf spark-3.0.1-bin-hadoop3.2.tgz
    #os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
    #os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
    os.environ["SPARK_HOME"] = "/content/spark-"+spark_versioin+"-bin-hadoop3.2"
    if 'findspark' in sys.modules.keys():
        print('findspark already installed')    
    else:
        print('installing findspark')
        !pip install -q findspark

def start_spark():
    import findspark
    findspark.init()
    from pyspark.sql import SparkSession
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
