# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots['test_invalid_dtc 1'] = '''
Error validating data dtc with schema blurr/core/syntax/dtc_streaming_schema.yml
\tStore.ReadWriteUnits: 'five!' is not a int.
'''
