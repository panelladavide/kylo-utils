# kylo-utils
Scripts to interact with kylo

To use this utility you have to put username and password in a .config file in the project directory.
for istance:
```bash
[configuration]
username = foo
password = xxxx
passwordTest = xxxx
```

#### abandon_feeds.py
Abandon all running jobs filtered by feed name
#### create_feed.py
Create multiple feeds based on the template.json. To create new ten feeds template launch this shell command:

```bash
for i in {1..10}; do sed 's/test_template/test_template_'${i}'/g' test_template.json > create_feeds/test_template${i}.json; done
```

Then launch create_feeds.py it will create 10 feeds on kylo based on the test_template.json

The schema of the table which will be created is like the `test_file.csv` file

#### create_opendata_table.py

#### delete_feeds.py
Delete all feeds by name

#### fail_all_feeds.py
Fail all feed running jobs by name

#### fail_all_feeds_test.py
Fail all feed running jobs by name

#### feeds_report.py
Write a report.csv with infos on feeds enabled on kylo
