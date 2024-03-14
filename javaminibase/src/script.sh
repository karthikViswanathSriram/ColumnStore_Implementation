echo "java interfaces/BatchInsert ../sampledata50000.txt mydb colfile 4"
java interfaces/BatchInsert ../sampledata50000.txt mydb colfile 4
if [ $1 == "BITMAP" ] || [ $1 == "BTREE" ]; then
    echo "java interfaces/Index mydb colfile C $1"
    java interfaces/Index mydb colfile C $1 
fi

echo "java interfaces/Query mydb colfile A,B,C 'C > 8' 100 ${1}"
java interfaces/Query mydb colfile A,B,C "C > 8" 100 ${1}

echo "java interfaces/DelQuery mydb colfile A,B,C 'C > 8' 100 $1 "NO PURGE""
java interfaces/DelQuery mydb colfile A,B,C "C > 8" 100 $1 "NO PURGE"

echo "java interfaces/Query mydb colfile A,B,C 'C > 8' 100 $1"
java interfaces/Query mydb colfile A,B,C "C > 8" 100 $1