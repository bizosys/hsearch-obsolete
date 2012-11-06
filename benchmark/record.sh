#echo Enter how many concurrent clients 
#read no
no=$1
echo "Number of concurrent clients are $no"

machines=$2
machines=`echo $machines | sed 's/,/ /g'`
curdir=`pwd`

for machine in $machines;
do 
	#ssh $machine "mkdir -p $curdir"
	$machine "mkdir -p $curdir"
done

for machine in $machines;
do 
	#ssh $machine /mnt/hsearch-0.90/benchmark/stat_on.sh $curdir $no
	$machine /mnt/hsearch-0.90/benchmark/stat_on.sh $curdir $no
done

query.sh  queries.txt 25 $no  1 > $no.txt 2>&1

for machine in $machines;
do 
	#ssh $machine /mnt/benchmark/stat_off.sh $curdir $no
	$machine /mnt/benchmark/stat_off.sh $curdir $no
done

tail -n 50 $no.txt

cd -
