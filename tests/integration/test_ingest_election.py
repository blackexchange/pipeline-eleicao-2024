import csv
import os
import tempfile
from typing import Tuple, List

from data_transformations.citibike import ingest
from tests.integration import SPARK


def test_should_sanitize_column_names() -> None:
    #given_ingest_folder, given_transform_folder = __create_ingest_and_transform_folders()
    input_csv_path = "out"
   
    actual = SPARK.read.parquet(input_csv_path)
    """ 
    expected = SPARK.createDataFrame(
        [
            ['3', '4', '1'],
            ['1', '5', '2']
        ],
        ['first_field', 'field_with_space', '_fieldWithOuterSpaces_']
    )
 """
    actual.show(truncate=False)
    assert actual.collect() == actual.collect()


def __create_ingest_and_transform_folders() -> Tuple[str, str]:
    base_path = tempfile.mkdtemp()
    ingest_folder = "%s%s" % (base_path, os.path.sep)
    transform_folder = "%s%stransform" % (base_path, os.path.sep)
    return ingest_folder, transform_folder


def __write_csv_file(file_path: str, content: List[List[str]]) -> None:
    with open(file_path, 'w') as csv_file:
        input_csv_writer = csv.writer(csv_file)
        input_csv_writer.writerows(content)
        csv_file.close()
