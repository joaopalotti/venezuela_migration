

input=$1
echo "Fixing $1"

python check_errors.py correct.csv $input

echo "Reissuing API calls..."
python reissue.py reissue.csv &> /dev/null

echo "Fixing dataframe..."
python fix_dataframe.py $input reissue.csv

echo "Moving from ${input}.fixed to ${input}"
mv ${input}.fixed ${input}

echo "Done!"

