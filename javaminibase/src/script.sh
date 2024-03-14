
echo "java interfaces/BatchInsert ../sampledata50000.txt mydb colfile 4"
java interfaces/Query mydb colfile A,B,C "C > 8" 100 ${1}
java interfaces/DelQuery mydb colfile A,B,C "C > 8" 100 $1 "NO PURGE"
java interfaces/Query mydb colfile A,B,C "C > 8" 100 $1