

input=$1
correct=$2

echo "Fixing $1 with $2"

python check_errors.py $correct $input

echo "Reissuing API calls..."
python reissue.py reissue.csv # &> /dev/null

echo "Fixing dataframe..."
python fix_dataframe.py $input reissue.csv

#echo "Replace ${correct} with ${input}.fixed"
#mv $correct ${correct}.old
#cp ${input}.fixed $correct

echo "Moving from ${input}.fixed to ${input}"
mv ${input}.fixed ${input}

echo "Done!"

