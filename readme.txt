药品注册变更增量汇聚服务

该项目用于接收不同登记来源的药品注册批次数据，按来源游标与业务主键汇总增量记录，并把可追溯的批次结果保存到本地 SQLite 数据库。演示入口读取 fixtures/demo.json，输出本次处理的新增、更新、重复和异常记录摘要。

测试：python3 -m unittest discover -s tests/registry_batches -p 'test_*.py'
构建：python3 -m compileall -q registry_delta_hub
验收：python3 -m registry_delta_hub.console.run fixtures/demo.json --db /tmp/registry_delta_hub-acceptance.sqlite3
