echo Ënter the data root directory :
read data

echo Enter the meta data sub folder:
read meta
echo Formatting $data/$meta
echo "Type Y to continue (Y/N):"
read option
if [ $option == "N" ]; then
	echo Ignoring formatting
	exit 1
fi

echo "Formatting  $data/$meta/dfsname/*"
rm -rf $data/$meta/dfsname/*
mkdir -p $data/$meta/dfsname
echo "Formatting  $data/$meta/dfsnameedit/*"
rm -rf $data/$meta/dfsnameedit/*
mkdir -p $data/$meta/dfsnameedit

echo "Enter data folders to be removed e.g. 1/* 2/* 3/*" 
read blocks
echo Formatting $blocks
echo "Type Y to continue (Y/N):"
read option
if [ $option == "N" ]; then
	echo Ignoring formatting
	exit 1
fi

cd $data
rm -rf $blocks
cd -
