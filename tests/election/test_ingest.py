import pytest


from data_transformations.election import ingest

@pytest.mark.skip()
def test_should_get_list_files() -> None:
   
    actual = ingest.list_totalization_files()

    assert len (actual) > 0

@pytest.mark.skip()
def test_should_download() -> bool:
   
    actual = ingest.download_file("https://cdn.tse.jus.br/estatistica/sead/eleicoes/eleicoes2024/arqurnatot/bu_imgbu_logjez_rdv_vscmr_2024_1t_TO.zip",'in','bu_imgbu_logjez_rdv_vscmr_2024_1t_TO.zip')

    assert actual == True

@pytest.mark.skip()
def test_should_extract_zip() -> bool:
   
    actual = ingest.unzip_file("/Users/user/though/pipe/in/bu_imgbu_logjez_rdv_vscmr_2024_1t_TO.zip","/Users/user/though/pipe/in/unzipped")

    assert actual == True

@pytest.mark.skip()
def test_should_extract_log() -> bool:
   
    actual = ingest.extract_log_text("/Users/user/though/pipe/in/unzipped/o00452to7300800070135-log.jez","/Users/user/though/pipe/in/logs")

    assert actual == True

def test_should_process_log_file() -> bool:
   
    actual = ingest.process_log_file("/Users/user/though/pipe/in/logs/logd.dat")

    assert actual == True
