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
echo $CLASSPATH

ulimit -n 16384
export CLASSPATH=$CLASSPATH:$HSEARCH_HOME/hsearch-0.90-benchmark.jar
. $HSEARCH_HOME/bin/setenv.sh
cd $HSEARCH_HOME
java -cp $HSEARCH_HOME/conf:$CLASSPATH  -Xmx2048m com.bizosys.hsearch.benchmark.HSearchWikipediaW $HSEARCH_HOME/../sampledata/enwiki.xml page id title revision redirect,revision,minor,revisiontimestamp,revisionid,revisioncontributorid,title,xml:space,revisioncontributorusername,revisiontext,revisioncomment,revisioncontributorip,restrictions,deleted 0 5000000 700 TokenizeStandard,ComputeTokens,SaveToIndex,SaveToPreview 1
