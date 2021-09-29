# Block 1
1. `hdfs dfs -mkdir /data`
2. `hdfs dfs -mkdir -p /data/level1/level2`
3. Trash это директория которая помогает предотвратить немедленное удаление файлов, 
   вместо этого файлы при удалении переносятся в нее
4. `hdfs dfs -touchz /data/level1/level2/nesterenko`
5. `hdfs dfs -rm -skipTrash /data/level1/level2/nesterenko`
6. `hdfs dfs -rm -r -skipTrash /data`
# Block 2
1. `hdfs dfs -put mapper.py /data`
2. `hdfs dfs -cat /data/mapper.py`
3. `hdfs dfs -cat /data/mapper.py | tail -n 2`
4. `hdfs dfs -cat /data/mapper.py | head -n 2`
5. `hdfs dfs -cp /data/mapper.py /data_copy`
# Block 3
1. `hdfs dfs -setrep 2 /data/mapper.py`  Время на увеличение и уменьшение числа реплик занимает 2-3 секунды
2. `hdfs fsck /data/mapper.py -files -blocks -locations`
3. `hdfs fsck -blockId blk_1073742162`