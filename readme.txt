测试：python3 -m unittest discover -s tests/registry_batches -p 'test_*.py'
构建：python3 -m compileall -q registry_delta_hub
验收：python3 -m registry_delta_hub.console.run fixtures/demo.json --db /tmp/registry_delta_hub-acceptance.sqlite3
