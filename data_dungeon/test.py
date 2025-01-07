from tests import test_only_redistribution as only
from tests import test_redistribution as red
from tests import test_adjusted_redistribution as adj

# selection of set to test
# 1 -> only redistribution
# 2 -> redistribution
# 3 -> adjusted redistribution
test = 3

if test == 1:
    only.test_perform_input_output()
    only.test_perform_block_transactions()
    only.test_perform_redistribution()
    only.test_perform_coinbase_transaction()
elif test == 2:
    red.test_perform_input_output()
    red.test_perform_block_transactions()
    red.test_perform_redistribution()
elif test == 3:
    adj.test_perform_input_output()
    adj.test_perform_block_transactions()
    adj.test_perform_redistribution()