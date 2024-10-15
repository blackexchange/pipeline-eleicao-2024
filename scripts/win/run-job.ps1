poetry build

$JOB=[System.Environment]::GetEnvironmentVariable('JOB')
$jobName=$JOB.ToLower()

switch($jobName)
{

    citibike_ingest {
            $INPUT_FILE_PATH="resources/citibike/citibike.csv"
            $JOB_ENTRY_POINT="jobs/citibike_ingest.py"
            $OUTPUT_PATH="./output_int"
            Break
            }
    citibike_distance_calculation {
                $INPUT_FILE_PATH="./output_int"
                $JOB_ENTRY_POINT="jobs/citibike_distance_calculation.py"
                $OUTPUT_PATH="./output"
                Break
                }
    wordcount {
            $INPUT_FILE_PATH="resources/word_count/words.txt"
            $JOB_ENTRY_POINT="jobs/word_count.py"
            $OUTPUT_PATH="./output"
            Break
            }

    election {
            $INPUT_FILE_PATH="./in/logs"
            $JOB_ENTRY_POINT="./jobs/election_ingest.py"
            $OUTPUT_PATH="./out"
            $UF="TO"
            Break
            }
            
    default {
              Write-Host "Job name provided was : ${JOB} : failed"
              Write-Host "Job name deduced was : ${jobName} : failed"
              Write-Host "Please enter a valid job name (citibike_ingest, citibike_distance_calculation or wordcount)"
              exit 1
              Break
            }


}

if (Test-Path $OUTPUT_PATH) {
    Remove-Item -Recurse -Force $OUTPUT_PATH
}

poetry run spark-submit --conf spark.pyspark.python="C:\Users\Neville\AppData\Local\pypoetry\Cache\virtualenvs\scrap-video-7xC0EiYu-py3.11\Scripts\python.exe" --master local --py-files dist/data_transformations-*.whl $JOB_ENTRY_POINT $INPUT_FILE_PATH $OUTPUT_PATH $UF

#poetry run spark-submit --master local --py-files dist/data_transformations-*.whl $JOB_ENTRY_POINT $INPUT_FILE_PATH $OUTPUT_PATH --conf "spark.pyspark.python=$(poetry run where python)"
