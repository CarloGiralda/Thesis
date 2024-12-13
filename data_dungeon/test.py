from tests import test_only_redistribution as only
from tests import test_redistribution as red

# selection of set to test
# 1 -> only redistribution
# 2 -> redistribution
test = 1

if test == 1:
    only.test_perform_input_output()
    only.test_perform_block_transactions()
    only.test_perform_redistribution()
    only.test_perform_coinbase_transaction()
elif test == 2:
    red.test_perform_input_output()
    red.test_perform_block_transactions()
    red.test_perform_redistribution()