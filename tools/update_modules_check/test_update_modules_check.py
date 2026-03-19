#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import unittest
import io
import sys
from update_modules_check import get_sub_it_modules


class TestGetSubItModules(unittest.TestCase):

    def _capture_output(self, modules_str, total_num, current_num):
        """Capture printed output from get_sub_it_modules."""
        captured = io.StringIO()
        sys.stdout = captured
        try:
            get_sub_it_modules(modules_str, total_num, current_num)
        finally:
            sys.stdout = sys.__stdout__
        return captured.getvalue().strip()

    def test_with_all_dedicated_modules_present(self):
        """Should work when all dedicated modules are in the list."""
        modules = (
            ",connector-jdbc-e2e,connector-kafka-e2e,connector-rocketmq-e2e,"
            "connector-kudu-e2e,connector-amazonsqs-e2e,connector-doris-e2e,"
            "connector-paimon-e2e,connector-cdc-oracle-e2e,connector-file-local-e2e,"
            "connector-file-sftp-e2e,connector-redis-e2e,connector-sensorsdata-e2e,"
            "connector-mysql-e2e,connector-postgres-e2e"
        )
        output = self._capture_output(modules, 2, 0)
        self.assertNotIn("connector-jdbc-e2e", output)
        self.assertNotIn("connector-kafka-e2e", output)

    def test_with_no_dedicated_modules_present(self):
        """Should not crash when none of the dedicated modules are in the list."""
        modules = ",connector-mysql-e2e,connector-postgres-e2e,connector-oracle-e2e"
        # Use total_num=1 to get all modules in one partition
        output = self._capture_output(modules, 1, 0)
        self.assertIn("connector-mysql-e2e", output)
        self.assertIn("connector-postgres-e2e", output)
        self.assertIn("connector-oracle-e2e", output)

    def test_with_partial_dedicated_modules(self):
        """Should not crash when only some dedicated modules are in the list."""
        modules = ",connector-jdbc-e2e,connector-mysql-e2e,connector-kafka-e2e"
        output = self._capture_output(modules, 1, 0)
        self.assertNotIn("connector-jdbc-e2e", output)
        self.assertNotIn("connector-kafka-e2e", output)
        self.assertIn("connector-mysql-e2e", output)

    def test_empty_input(self):
        """Should handle empty-ish input without crashing."""
        modules = ","
        output = self._capture_output(modules, 1, 0)
        self.assertEqual("", output)


if __name__ == "__main__":
    unittest.main()
