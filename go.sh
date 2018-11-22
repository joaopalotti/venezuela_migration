
f=$1
correct="corrects/correct_main.csv"

./fix_dataframe.sh $f $correct

gzip $f
mv $f.gz collections/colombia/ 

s="Fixed collection $(date '+%B %d')"
git add collections/colombia/$f.gz

git commit -m "$s"


