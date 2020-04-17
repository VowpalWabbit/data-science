from __future__ import print_function
import datetime
import io
import os
import sys
import time
import config
try:
    input = raw_input
except NameError:
    pass

import common.helpers
import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batch_auth
import azure.batch.models as batchmodels

sys.path.append('.')
sys.path.append('..')


# Update the Batch and Storage account credential strings in config.py with values
# unique to your accounts. These are used when constructing connection strings
# for the Batch and Storage client objects.

def query_yes_no(question, default="yes"):
    """
    Prompts the user for yes/no input, displaying the specified question text.

    :param str question: The text of the prompt for input.
    :param str default: The default if the user hits <ENTER>. Acceptable values
    are 'yes', 'no', and None.
    :rtype: str
    :return: 'yes' or 'no'
    """
    valid = {'y': 'yes', 'n': 'no'}
    if default is None:
        prompt = ' [y/n] '
    elif default == 'yes':
        prompt = ' [Y/n] '
    elif default == 'no':
        prompt = ' [y/N] '
    else:
        raise ValueError("Invalid default answer: '{}'".format(default))

    while 1:
        choice = input(question + prompt).lower()
        if default and not choice:
            return default
        try:
            return valid[choice[0]]
        except (KeyError, IndexError):
            print("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")


def print_batch_exception(batch_exception):
    """
    Prints the contents of the specified Batch exception.

    :param batch_exception:
    """
    print('-------------------------------------------')
    print('Exception encountered:')
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        print(batch_exception.error.message.value)
        if batch_exception.error.values:
            print()
            for mesg in batch_exception.error.values:
                print('{}:\t{}'.format(mesg.key, mesg.value))
    print('-------------------------------------------')


def get_file_url_from_container(block_blob_client, container_name, blob_name):
    """
    Uploads a local file to an Azure Blob storage container.

    :param block_blob_client: A blob service client.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str container_name: The name of the Azure Blob storage container.
    :param str file_path: The local path to the file.
    :rtype: `azure.batch.models.ResourceFile`
    :return: A ResourceFile initialized with a SAS URL appropriate for Batch
    tasks.
    """
    #blob_name = os.path.basename(file_path)

    #print('Uploading file {} to container [{}]...'.format(file_path,
               #                                           container_name))

    #block_blob_client.create_blob_from_path(container_name,
    #                                       blob_name,
    #                                      file_path)

    sas_token = block_blob_client.generate_blob_shared_access_signature(
        container_name,
        blob_name,
        permission=azureblob.BlobPermissions.READ,
        expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2))

    sas_url = block_blob_client.make_blob_url(container_name,
                                              blob_name,
                                              sas_token=sas_token)

    return batchmodels.ResourceFile(http_url=sas_url, file_path=blob_name)

'''
def download_from_blob(block_blob_client, storage_container_name, storage_file_name, local_file_path, throw_ex = False):
    try:
        print("\nDownloading from Blob storage to file")
        t1 = datetime.now()
        block_blob_client.get_blob_to_path(storage_container_name, storage_file_name, local_file_path)
        t2 = datetime.now()
        print(storage_file_name)
        print("Done downloading blob")
        print('Download Time:',(t2-t1)-timedelta(microseconds=(t2-t1).microseconds))
    except Exception as e:
        print(e)
        if throw_ex: raise(e)
'''

def get_container_sas_token(block_blob_client,
                            container_name, blob_permissions):
    """
    Obtains a shared access signature granting the specified permissions to the
    container.

    :param block_blob_client: A blob service client.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str container_name: The name of the Azure Blob storage container.
    :param BlobPermissions blob_permissions:
    :rtype: str
    :return: A SAS token granting the specified permissions to the container.
    """
    # Obtain the SAS token for the container, setting the expiry time and
    # permissions. In this case, no start time is specified, so the shared
    # access signature becomes valid immediately.
    container_sas_token = \
        block_blob_client.generate_container_shared_access_signature(
            container_name,
            permission=blob_permissions,
            expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2))

    return container_sas_token


def create_pool(batch_service_client, pool_id):
    """
    Creates a pool of compute nodes with the specified OS settings.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str pool_id: An ID for the new pool.
    :param str publisher: Marketplace image publisher
    :param str offer: Marketplace image offer
    :param str sku: Marketplace image sku
    """
    print('Creating pool [{}]...'.format(pool_id))

    # Create a new pool of Linux compute nodes using an Azure Virtual Machines
    # Marketplace image. For more information about creating pools of Linux
    # nodes, see:
    # https://azure.microsoft.com/documentation/articles/batch-linux-nodes/
    
    # Specify the commands for the pool's start task. The start task is run
    # on each node as it joins the pool, and when it's rebooted or re-imaged.
    # We use the start task to prep the node for running our task script.
    task_commands = [
    # Install pip
    'yes | sudo apt-get install python3-distutils 2> /dev/null',
    'curl -fSsL https://bootstrap.pypa.io/get-pip.py',
    'yes | python3 get-pip.py',
    # Install the azure-storage module so that the task script can access
    # Azure Blob storage, pre-cryptography version
    'yes | sudo pip install azure-storage']
    user = batchmodels.AutoUserSpecification(scope=batchmodels.AutoUserScope.pool,elevation_level=batchmodels.ElevationLevel.admin)
    new_pool = batch.models.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=batchmodels.ImageReference(
                #publisher="Canonical",
                #offer="UbuntuServer",
                #sku="18.04-LTS",
                #version="latest"
                #/subscriptions/{subscriptionId}/resourceGroups/{resourceGroup}/providers/Microsoft.Compute/images/{imageName}
                virtual_machine_image_id="/subscriptions/9a2d7383-3c7d-492c-94fc-ba65be408672/resourceGroups/cps-dev-ataymano/providers/Microsoft.Compute/images/ataymanovwvm2-image-20190524103441"
                ),
        node_agent_sku_id="batch.node.ubuntu 18.04"),
        vm_size=config._POOL_VM_SIZE,
        target_dedicated_nodes=config._DEDICATED_POOL_NODE_COUNT,
        target_low_priority_nodes=config._LOW_PRIORITY_POOL_NODE_COUNT,
        start_task=batch.models.StartTask(command_line=common.helpers.wrap_commands_in_shell('linux',task_commands),user_identity=batchmodels.UserIdentity(auto_user=user),
        wait_for_success=True
        )
        
    )
    batch_service_client.pool.add(new_pool)
    


def create_job(batch_service_client, job_id, pool_id):
    """
    Creates a job with the specified ID, associated with the specified pool.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID for the job.
    :param str pool_id: The ID for the pool.
    """
    print('Creating job [{}]...'.format(job_id))

    job = batch.models.JobAddParameter(
        id=job_id,
        pool_info=batch.models.PoolInformation(pool_id=pool_id))

    batch_service_client.job.add(job)

def add_tasks(batch_service_client, job_id, input_file, data_files_paths,vw, storage_account, storage_key, container_name):
    """
    Adds a task for each input file in the collection to the specified job.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID of the job to which to add the tasks.
    :param list input_files: A collection of input files. One task will be
     created for each input file.
    :param output_container_sas_token: A SAS token granting write access to
    the specified Azure Blob storage container.
    """

    print('Adding {} task to job [{}]...'.format(1, job_id))

    tasks = list()

    for idx, data_file_path in enumerate(data_files_paths):
        try:
            print("Downloading file from azure storage")
        except Exception as e:
            print(e)

        task_start_time = time.time()
        command = "/bin/bash -c \"python3 {} \\\"{}\\\" \\\"{}\\\" {} {} \\\"{}\\\" \\\"{}\\\" {} {}\"".format(input_file.file_path, data_file_path.http_url, vw.http_url, 0, 1, storage_account, storage_key, results_container_name, experiment_name)
        print("command is ", command)
    
        tasks.append(batch.models.TaskAddParameter(
            id='Task{}'.format(idx),
            command_line=command,
            resource_files=[input_file]
            )
        )
        
        print('Task {} run time {}'.format(idx,time.time() - task_start_time))

    batch_service_client.task.add_collection(job_id, tasks)

    

def wait_for_tasks_to_complete(batch_service_client, job_id, timeout):
    """
    Returns when all tasks in the specified job reach the Completed state.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The id of the job whose tasks should be to monitored.
    :param timedelta timeout: The duration to wait for task completion. If all
    tasks in the specified job do not reach Completed state within this time
    period, an exception will be raised.
    """
    timeout_expiration = datetime.datetime.now() + timeout

    print("Monitoring all tasks for 'Completed' state, timeout in {}..."
          .format(timeout), end='')

    while datetime.datetime.now() < timeout_expiration:
        print('.', end='')
        sys.stdout.flush()
        tasks = batch_service_client.task.list(job_id)

        incomplete_tasks = [task for task in tasks if
                            task.state != batchmodels.TaskState.completed]
        if not incomplete_tasks:
            print()
            return True
        else:
            time.sleep(1)

    print()
    raise RuntimeError("ERROR: Tasks did not reach 'Completed' state within "
                       "timeout period of " + str(timeout))


def print_task_output(batch_service_client, job_id, encoding=None):
    """Prints the stdout.txt file for each task in the job.

    :param batch_client: The batch client to use.
    :type batch_client: `batchserviceclient.BatchServiceClient`
    :param str job_id: The id of the job with task output files to print.
    """
    
    print('Printing task output...')

    tasks = batch_service_client.task.list(job_id)

    for task in tasks:

        node_id = batch_service_client.task.get(job_id, task.id).node_info.node_id
        print("Task: {}".format(task.id))
        print("Node: {}".format(node_id))

        stream = batch_service_client.file.get_from_task(job_id, task.id, config._STANDARD_OUT_FILE_NAME)

        file_text = _read_stream_as_string(
            stream,
            encoding)
        print("Standard output:")
        print(file_text)

def _read_stream_as_string(stream, encoding):
    """Read stream as string

    :param stream: input stream generator
    :param str encoding: The encoding of the file. The default is utf-8.
    :return: The file content.
    :rtype: str
    """
    output = io.BytesIO()
    try:
        for data in stream:
            output.write(data)
        if encoding is None:
            encoding = 'utf-8'
        return output.getvalue().decode(encoding)
    finally:
        output.close()
    raise RuntimeError('could not write data to stream or decode bytes')
    

if __name__ == '__main__':

    start_time = datetime.datetime.now().replace(microsecond=0)
    print('Sample start: {}'.format(start_time))
    print()

    # Create the blob client, for use in obtaining references to
    # blob storage containers and uploading files to containers.

    
    blob_client = azureblob.BlockBlobService(
        account_name=config._STORAGE_ACCOUNT_NAME,
        account_key=config._STORAGE_ACCOUNT_KEY)

    # Use the blob client to create the containers in Azure Storage if they
    # don't yet exist.
    
    # your storage account must have all containers listed below set up
    data_container_name = 'omltest'# 'oml' has all the 500 datasets, you can pass 'omltest' to test your script on one dataset
    scripts_container_name = 'scripts' # make sure the script you want to run in line 386 is this folder 'scripts'
    vw_container_name = 'vwexecutables' #'vwexecutables'
    results_container_name = 'results' #
    experiment_name = config._EXPERIMENT_NAME
    
    blob_client.create_container(data_container_name, fail_on_exist=False)

    files = blob_client.list_blobs(data_container_name)
    
    unsorted_data_files = {}
    for blob in files:
        if "ds" in blob.name:  
            print("Appending {} to files to be processed".format(blob.name))
            length = azureblob.BlockBlobService.get_blob_properties(blob_client,data_container_name,blob.name).properties.content_length
            unsorted_data_files[blob.name] = length
    
    data_files = list(sorted(unsorted_data_files, key=unsorted_data_files.__getitem__, reverse=True))
    #print(data_files)
    
    # The collection of data files that are to be processed by the tasks.
    #input_file_paths =  [os.path.join(sys.path[0], 'taskdata0.txt'),
    #                     os.path.join(sys.path[0], 'taskdata1.txt'),
    #                     os.path.join(sys.path[0], 'taskdata2.txt')]

    # Upload the data files. 
    
    # "run_vw_job.py must be in the ontainer specified in scripts_container_name
    input_file = get_file_url_from_container(blob_client, scripts_container_name, "run_vw_job.py")

    data_files_paths = [get_file_url_from_container(blob_client, data_container_name, file_to_process) for file_to_process in data_files]

    vw = get_file_url_from_container(blob_client, vw_container_name, "vw")


    # Create a Batch service client. We'll now be interacting with the Batch
    # service in addition to Storage
    credentials = batch_auth.SharedKeyCredentials(config._BATCH_ACCOUNT_NAME,
                                                 config._BATCH_ACCOUNT_KEY)

    batch_client = batch.BatchServiceClient(
        credentials,
        batch_url=config._BATCH_ACCOUNT_URL
        )

    try:
        # Create the pool that will contain the compute nodes that will execute the
        # tasks.
        # For now commenting pool creation as I am specifying an already created pool usin custom VM
        # create_pool(batch_client, config._POOL_ID)
        
        # Create the job that will run the tasks.
        create_job(batch_client, config._JOB_ID, config._POOL_ID)

        # Add the tasks to the job. 
        add_tasks(batch_client, config._JOB_ID, input_file, data_files_paths, vw, config._STORAGE_ACCOUNT_NAME, config._STORAGE_ACCOUNT_KEY,data_container_name)

        # Pause execution until tasks reach Completed state.
        wait_for_tasks_to_complete(batch_client,
                               config._JOB_ID,
                               datetime.timedelta(minutes=30))

        print("  Success! All tasks reached the 'Completed' state within the "
          "specified timeout period.")

        # Print the stdout.txt and stderr.txt files for each task to the console
        print_task_output(batch_client, config._JOB_ID)

    except batchmodels.BatchErrorException as err:
        print_batch_exception(err)
        raise

    # Clean up storage resources
    #print('Deleting container [{}]...'.format(input_container_name))
    #blob_client.delete_container(input_container_name)

    # Print out some timing info
    end_time = datetime.datetime.now().replace(microsecond=0)
    print()
    print('Sample end: {}'.format(end_time))
    print('Elapsed time: {}'.format(end_time - start_time))
    print()

    # Clean up Batch resources (if the user so chooses).
    if query_yes_no('Delete job?') == 'yes':
        batch_client.job.delete(config._JOB_ID)

    if query_yes_no('Delete pool?') == 'yes':
        batch_client.pool.delete(config._POOL_ID)

    print()
    input('Press ENTER to exit...')
