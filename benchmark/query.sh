this="$0"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin"; pwd`
this="$bin/$script"

# the root of the Hadoop installation
export HSEARCH_HOME=`dirname "$this"`/..
echo $HSEARCH_HOME

for lib in $HSEARCH_HOME/build/plugins/service-crawler/*.jar;
do 
	export CLASSPATH=$CLASSPATH:$lib
done

ulimit -n 16384
. $HSEARCH_HOME/bin/setenv.sh
cd $HSEARCH_HOME
java -cp conf:$CLASSPATH  -server -XX:+UseParallelGC -XX:ParallelGCThreads=4 -XX:+AggressiveHeap -XX:+HeapDumpOnOutOfMemoryError -Xmx2048m com.bizosys.hsearch.benchmark.HSearchRead $HSEARCH_HOME/benchmark/queries.txt 0 1 1
cd bin
