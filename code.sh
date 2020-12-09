k=0
while read line;
do
	let "k+=1"
	echo $k  
	echo $line;
	wget -c $line;
	echo "Done"
done < links.txt
