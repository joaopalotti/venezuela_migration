
f=$1
correct="corrects/correct_main.csv"

echo "FIRST RUN..."
./fix_dataframe.sh $f $correct

echo "SECOND RUN..."
./fix_dataframe.sh $f $correct

echo "THIRD RUN..."
./fix_dataframe.sh $f $correct

echo "Replace ${correct} with ${input}.fixed"
mv $correct ${correct}.old
cp $f $correct

gzip $f
mv $f.gz collections/colombia/ 

s="Fixed collection $(date '+%B %d')"
git add collections/colombia/$f.gz

git commit -m "$s"


