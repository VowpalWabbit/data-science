# Global constant variables (Azure Storage account/Batch details)

# import "config.py" in "run_azure_batch_job.py "
import numpy as np
import time

np.random.seed(int(time.time()))


# batch account info
_BATCH_ACCOUNT_NAME ='' # Your batch account name 
_BATCH_ACCOUNT_KEY = '' # Your batch account key
_BATCH_ACCOUNT_URL = '' # Your batch account URL

# storage account info
_STORAGE_ACCOUNT_NAME = '' # Your storage account name
_STORAGE_ACCOUNT_KEY = ''# Your storage account keys


# other script specific configs
# you can skip the pool creation step in line 407 of run_azure_batch_job.py and instead set up a custom pool
# Specifying custom pool is recommended if the VM you want to use needs specific libraries installed (use a custom VM image for your pool set up)
# If you skip pool creation, please specify the pool ID below
_POOL_ID = "CBB3" # your pool name, it can be anything
_DEDICATED_POOL_NODE_COUNT = 1 # Pool node count
_LOW_PRIORITY_POOL_NODE_COUNT = 1
_POOL_VM_SIZE = 'STANDARD_A1_v2' # VM Type/Size
_JOB_ID = 'PythonQuickstartJob'+str(np.random.randint(1,100)) # Job ID
_STANDARD_OUT_FILE_NAME = 'stdout.txt' # Standard Output file
_EXPERIMENT_NAME = 'run_vw_job_test' # no spaces please
