#!/bin/bash
n=0
stat=0
#TestBasic
#TestConcurrent
#TestUnreliable
#TestUnreliableOneKey
#TestOnePartition
#TestManyPartitionsOneClient
#TestManyPartitionsManyClients
#TestPersistOneClient
#TestPersistConcurrent
#TestPersistConcurrentUnreliable
#TestPersistPartition
#TestPersistPartitionUnreliable

until [ $stat -eq 1 ]
do
    go test --run 'TestBasic$' &> $1
    go test --run 'TestConcurrent$' &>> $1
    go test --run 'TestUnreliable$' &>> $1
    go test --run 'TestUnreliableOneKey$'  &>> $1
    go test --run 'TestOnePartition$' &>> $1
    go test --run 'TestManyPartitionsOneClient$' &>> $1
    go test --run 'TestManyPartitionsManyClients$' &>> $1
    go test --run 'TestPersistOneClient$' &>> $1
    go test --run 'TestPersistConcurrent$' &>> $1
    go test --run 'TestPersistConcurrentUnreliable$' &>> $1
    go test --run 'TestPersistPartition$' &>> $1
    go test --run 'TestPersistPartitionUnreliable$' &>> $1
    grep -rn FAIL $1
    if [ $? -ne 1 ]
    then
	echo "some failures in following file" 
	echo $1
	stat=1
    fi
    n=$[$n+1]
    echo "round $n complete.."
    sleep 1
done

echo $n complete
exit $stat
