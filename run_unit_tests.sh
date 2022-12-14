echo '----========LwDP v2 tests========----'
pip install th2_grpc_lw_data_provider==2.0.0.dev3656626108 -U -q
pip list
python3 -m pytest tests/tests_unit

