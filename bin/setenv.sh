export JAVA_HOME=/usr/java/jdk1.6.0
export PATH=$PATH:$JAVA_HOME/bin

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
echo "HSearch Home:" $HSEARCH_HOME

for lib in $HSEARCH_HOME/lib/*.jar;
do 
	export CLASSPATH=$lib:$CLASSPATH
done

for lib in $HSEARCH_HOME/*.jar;
do 
	export CLASSPATH=$lib:$CLASSPATH
done

export CLASSPATH=$CLASSPATH:$HSEARCH_HOME/conf
echo $CLASSPATH
