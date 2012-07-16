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

ulimit -n 16384
rm -rf HSEARCH_HOME/hadoop/logs/*
$HSEARCH_HOME/hadoop/bin/start-dfs.sh
sleep 2
netstat -l -pN --tcp
