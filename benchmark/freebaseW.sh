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
java -cp $HSEARCH_HOME/conf:$CLASSPATH  -Xmx2048m com.bizosys.hsearch.benchmark.HSearchFreebaseW $HSEARCH_HOME/benchmark/location.tsv id name name,geolocation,containedby name,geolocation,containedby,area,time_zones,geometry,contains,people_born_here,adjoin_s,usbg_name,adjectival_form,coterminous_with,street_address 1 1000000 5001 TokenizeStandard,FilterStopwords,FilterTermLength,FilterLowercase,FilterStem,ComputeTokens,SaveToDictionary,SaveToIndex,SaveToPreview,SaveToDetail 1
cd bin
sleep 2
