# /usr/bin/bash
total_count=0

echo "printing all java files"

for file in ./*/*.java
do
    wc -l $file
done

