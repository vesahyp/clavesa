import json
import sys

from databricks.sdk.runtime import dbutils

letters = json.loads(sys.argv[1])
dbutils.jobs.taskValues.set(key="n", value=len(letters))
